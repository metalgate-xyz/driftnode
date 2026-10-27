package daemon

import (
	"driftnode/internal/core"
	"driftnode/internal/proto/driftnodepb"
	syncproto "driftnode/internal/sync"
	"time"
)

// zenInfo describes a known zen and its connection state.
type zenInfo struct {
	ID       string `json:"id"`       // tailcat token of the zen
	Identity string `json:"identity"` // driftnode:<pubkey> once learned, else empty
	Name     string `json:"name"`     // zen name once known, else empty
	Kind     string `json:"kind"`     // native_zen or browser
	Status   string `json:"status"`   // connecting, connected, discovered, error
	Verified bool   `json:"verified"` // identity confirmed out-of-band (§7)
}

// rehydrateZens loads the persisted network into the in-memory zens map:
// the routing bindings (identity -> token) as connecting entries, and the
// persisted token LRU as discovered entries. Called from Start so the known
// network is visible to the zens RPC right after a restart, instead of
// appearing empty until the head/tail lanes redial or an exchange relearns
// each token.
func (d *Daemon) rehydrateZens() {
	routing, err := d.store.AllRouting()
	if err != nil {
		d.logger.Warn("rehydrate zens: read routing", "err", err)
		return
	}
	tokens, err := d.store.KnownTokens()
	if err != nil {
		tokens = nil
		d.logger.Warn("rehydrate zens: read known tokens", "err", err)
	}
	if len(routing) == 0 && len(tokens) == 0 {
		return
	}
	d.mu.Lock()
	for id, tok := range routing {
		if _, exists := d.zens[tok]; exists {
			continue
		}
		d.zens[tok] = &zenInfo{
			ID:       tok,
			Identity: string(id),
			Kind:     "native_zen",
			Status:   "connecting",
		}
		if name, _ := d.store.DisplayName(id); name != "" {
			d.zens[tok].Name = name
		}
		if v, _ := d.store.IsVerified(id); v {
			d.zens[tok].Verified = true
		}
	}
	// Tokens persisted by the LRU but not bound in routing were learned
	// through zen exchange; restore them as discovered entries.
	routed := make(map[string]bool, len(routing))
	for _, tok := range routing {
		routed[tok] = true
	}
	for _, tok := range tokens {
		if routed[tok] {
			continue
		}
		if _, exists := d.zens[tok]; exists {
			continue
		}
		d.zens[tok] = &zenInfo{ID: tok, Kind: "native_zen", Status: "discovered"}
	}
	d.mu.Unlock()
}

// zenToProto copies one zenInfo into a fresh proto Zen, enriching it with
// the name/identity/verified state bound to its token. It takes the zenInfo
// by value so the caller can copy it under d.mu and then enrich without the
// lock, avoiding a race on fields a concurrent setZenStatus may mutate.
func (d *Daemon) zenToProto(z zenInfo) *driftnodepb.Zen {
	out := zenToProtoPlain(z)
	out.Verified = false
	if id, ok, _ := d.store.RoutingByToken(z.ID); ok {
		out.Identity = string(id)
		if name, _ := d.store.DisplayName(id); name != "" {
			out.Name = name
		}
		if v, _ := d.store.IsVerified(id); v {
			out.Verified = true
		}
	}
	return out
}

// zenToProtoPlain copies a zenInfo value into a proto Zen with no enrichment.
func zenToProtoPlain(z zenInfo) *driftnodepb.Zen {
	return &driftnodepb.Zen{
		Id:       z.ID,
		Identity: z.Identity,
		Name:     z.Name,
		Kind:     z.Kind,
		Status:   z.Status,
		Verified: z.Verified,
	}
}

// tokenForIdentity returns the token of a known zen whose learned identity
// matches, if any. Used so a follow-by-pubkey can dial a zen that was
// already discovered via exchange without waiting for the sync loop.
func (d *Daemon) tokenForIdentity(identity core.Identity) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for tok, p := range d.zens {
		if core.Identity(p.Identity) == identity {
			return tok, true
		}
	}
	return "", false
}

// knownRefsList returns this node's known zen refs for zen exchange. It
// includes this node's own listener address (with identity and name) so
// zens can dial it back and discover it through the exchange. Each ref
// carries the identity and name when they are known, so the receiver can
// display a discovered zen by name without dialing it.
func (d *Daemon) knownRefsList() []syncproto.ZenRef {
	ownID, _, _ := d.store.Identity()
	tokens, err := d.store.KnownTokens()
	if err != nil {
		d.logger.Warn("known tokens read failed", "err", err)
		tokens = nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]syncproto.ZenRef, 0, len(tokens)+1)
	ownAddr := ""
	if d.tcListener != nil {
		ownAddr = string(d.tcListener.Addr())
		ref := syncproto.ZenRef{Token: ownAddr}
		if ownID != "" {
			ref.Identity = ownID
			if name, _ := d.store.DisplayName(ownID); name != "" {
				ref.Name = name
			}
		}
		out = append(out, ref)
	}
	for _, tok := range tokens {
		if tok == ownAddr {
			continue
		}
		ref := syncproto.ZenRef{Token: tok}
		// Prefer the routing binding (authoritative): a followed zen's
		// identity is bound and its name comes from the crawl cache.
		if id, ok, _ := d.store.RoutingByToken(tok); ok {
			ref.Identity = id
			if name, _ := d.store.DisplayName(id); name != "" {
				ref.Name = name
			}
		} else if p, ok := d.zens[tok]; ok {
			// Discovered zen: no routing binding, but the exchange that
			// learned this token may have carried identity/name hints
			// that learnZenRefs stored on the zen entry. Forward them so
			// names propagate transitively beyond followed zens.
			ref.Identity = core.Identity(p.Identity)
			ref.Name = p.Name
		}
		out = append(out, ref)
	}
	return out
}

// learnToken records a token mention in the persisted token LRU, refreshing
// its last-mention time. Called after every successful session, so tokens
// held hot by peers stay hot here too.
func (d *Daemon) learnToken(token string) error {
	if token == "" {
		return nil
	}
	return d.store.LearnToken(token, time.Now().UnixNano())
}

// learnZenRefs records the zen refs learned from one zen-exchange message.
// The tokens go into the persisted token LRU in a single batch (for
// zen-exchange offers and dedup); the zens map (for display) is updated
// per ref, but entries are NOT added to the auto-dial set, so they are not
// auto-dialed. When a ref carries an identity, it seeds the crawler so the
// ProfileLog is fetched authoritatively on the next crawl, and the name
// hint is stored for display until the crawl confirms it. Dialing a
// discovered zen requires explicit intent: a follow by token or a
// follow-resolved dial.
func (d *Daemon) learnZenRefs(refs []syncproto.ZenRef) {
	tokens := make([]string, 0, len(refs))
	for _, ref := range refs {
		if ref.Token == "" {
			continue
		}
		tokens = append(tokens, ref.Token)
	}
	if len(tokens) > 0 {
		if err := d.store.LearnTokens(tokens, time.Now().UnixNano()); err != nil {
			d.logger.Warn("learn zen tokens failed", "count", len(tokens), "err", err)
		}
	}
	for _, ref := range refs {
		if ref.Token == "" {
			continue
		}
		d.mu.Lock()
		_, exists := d.zens[ref.Token]
		d.mu.Unlock()
		if !exists {
			d.upsertZen(ref.Token, "native_zen", "discovered")
		}
		if ref.Identity != "" {
			// Seed the crawler so the signed ProfileLog is fetched on
			// the next crawl, confirming the name hint
			// authoritatively.
			d.mu.Lock()
			if d.crawler != nil {
				if id, err := core.ParseIdentity(string(ref.Identity)); err == nil {
					d.crawler.Seed(id)
				}
			}
			d.mu.Unlock()
			// Record the identity on the zen entry so it displays even
			// before the crawl fetches the name. A token can arrive
			// first as a bare token and later pick up an identity hint
			// from another exchange, so update even when the token was
			// already known. Only set the name when the ref carries one,
			// so an identity-only ref does not clobber a name learned
			// earlier.
			d.mu.Lock()
			if p, ok := d.zens[ref.Token]; ok {
				p.Identity = string(ref.Identity)
				if ref.Name != "" {
					p.Name = ref.Name
				}
			}
			d.mu.Unlock()
		}
	}
}

// upsertZen records a zen in the given state, overwriting any existing entry.
func (d *Daemon) upsertZen(id, kind, status string) {
	z := zenInfo{ID: id, Kind: kind, Status: status}
	d.mu.Lock()
	d.zens[id] = &z
	snap := z
	d.mu.Unlock()
	d.emitZenUpsert(snap)
}

// removeZen removes a zen record.
func (d *Daemon) removeZen(id string) {
	d.mu.Lock()
	delete(d.zens, id)
	d.mu.Unlock()
	d.emitZenRemove(id)
}

// setZenStatus updates a recorded zen's status.
func (d *Daemon) setZenStatus(id, status string) {
	d.mu.Lock()
	p, ok := d.zens[id]
	var snap zenInfo
	if ok {
		p.Status = status
		snap = *p
	}
	d.mu.Unlock()
	if ok {
		d.emitZenUpsert(snap)
	}
}
