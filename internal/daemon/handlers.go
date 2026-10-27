package daemon

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"driftnode/internal/core"
	"driftnode/internal/proto/driftnodepb"
	"driftnode/internal/store"
)

// signingKey returns the key to sign with. If the caller supplies a
// passphrase, it decrypts the at-rest key (and caches the result so later
// calls can omit the passphrase). Otherwise it returns the cached key, or an
// error if the key is locked. An idle-lock that has elapsed since the last
// signing call clears the key first.
func (d *Daemon) signingKey(passphrase string) (*core.KeyPair, error) {
	if passphrase != "" {
		return d.handleUnlock(passphrase)
	}
	d.mu.Lock()
	kp := d.unlocked
	if kp != nil && d.idleLock > 0 && time.Since(d.lastSignAt) > d.idleLock {
		d.unlocked = nil
		kp = nil
	}
	d.mu.Unlock()
	if kp == nil {
		return nil, errors.New("key is locked; run 'driftnode daemon unlock'")
	}
	return kp, nil
}

// handleUnlock decrypts the private key with the passphrase and caches it for
// subsequent keyless signing calls. A wrong passphrase leaves any previously
// unlocked key intact.
func (d *Daemon) handleUnlock(passphrase string) (*core.KeyPair, error) {
	ek, err := d.store.EncryptedKey()
	if err != nil {
		return nil, fmt.Errorf("read key: %w", err)
	}
	enc := core.DefaultKeyEncryption()
	priv, err := enc.Decrypt(ek, []byte(passphrase))
	if err != nil {
		return nil, errors.New("decrypt key: invalid passphrase")
	}
	kp, err := core.KeyPairFromBytes(priv)
	if err != nil {
		return nil, fmt.Errorf("keypair: %w", err)
	}
	d.mu.Lock()
	d.unlocked = kp
	d.lastSignAt = time.Now()
	bootstrapPath := d.bootstrapPath
	bootstrapKey := d.bootstrapVerifyKey
	bootstrapDone := d.bootstrapDone
	d.mu.Unlock()
	d.logger.Info("key unlocked")
	// If the bootstrap auto-follow ran while the key was locked, retry
	// it now that the key is available so seeds enter the follow graph.
	if !bootstrapDone && bootstrapPath != "" {
		go d.dialBootstrapSeeds(bootstrapPath, bootstrapKey)
	}
	d.emitStatus()
	return kp, nil
}

// handlePost signs a Post event with the given key and appends it to the
// local PostLog. When parentID is non-empty, the post is a reply to the
// referenced event.
func (d *Daemon) handlePost(text, parentID string, kp *core.KeyPair) (core.EventID, error) {
	var parent core.EventID
	if parentID != "" {
		p, err := core.ParseEventID(parentID)
		if err != nil {
			return core.EventID{}, fmt.Errorf("parent id: %w", err)
		}
		parent = p
	}
	post := &core.Post{Text: text}
	if !parent.IsZero() {
		exists, err := d.store.EventExists(parent)
		if err != nil {
			return core.EventID{}, err
		}
		if !exists {
			return core.EventID{}, errors.New("post not found locally")
		}
		post.ParentID = parent
	}
	ch, err := d.store.EnqueuePostEvent(kp, core.PostLog, core.Event{
		Kind: core.KindPost,
		Post: post,
	})
	if err != nil && !errors.Is(err, store.ErrHeld) {
		return core.EventID{}, err
	}
	res := <-ch
	if res.Err != nil {
		return core.EventID{}, res.Err
	}
	if res.SE == nil {
		return res.ID, nil
	}
	d.invalidateFeed()
	d.touchSignAt()
	d.triggerCrawlNow()
	ownID, _, _ := d.store.Identity()
	name := d.displayName(ownID)
	d.emitFeedAdd([]feedItem{{
		ID:        res.ID.String(),
		Timestamp: res.SE.Event.Timestamp,
		Author:    ownID.String(),
		Name:      name,
		Text:      text,
	}})
	return res.ID, nil
}

// handleLike signs a Like event targeting the given post event ID and
// appends it to the local PostLog. The like is recorded in the liker's own
// log, never the target's (section 7.2): nobody can write into someone
// else's log without a valid signature from that identity's key.
func (d *Daemon) handleLike(postID string, kp *core.KeyPair) (core.EventID, error) {
	target, err := core.ParseEventID(postID)
	if err != nil {
		return core.EventID{}, fmt.Errorf("post id: %w", err)
	}
	if target.IsZero() {
		return core.EventID{}, errors.New("post id required")
	}
	exists, err := d.store.EventExists(target)
	if err != nil {
		return core.EventID{}, err
	}
	if !exists {
		return core.EventID{}, errors.New("post not found locally")
	}
	ch, err := d.store.EnqueuePostEvent(kp, core.PostLog, core.Event{
		Kind: core.KindLike,
		Like: &core.Like{TargetID: target},
	})
	if err != nil && !errors.Is(err, store.ErrHeld) {
		return core.EventID{}, err
	}
	res := <-ch
	if res.Err != nil {
		return core.EventID{}, res.Err
	}
	if res.SE != nil {
		d.invalidateFeed()
		d.touchSignAt()
		d.triggerCrawlNow()
	}
	return res.ID, nil
}

// whoamiInfo is the local identity plus the current profile and detail
// projected from the own logs, so a user can see what they've filled in
// without a separate command.
type whoamiInfo struct {
	Identity    core.Identity
	Token       string
	Stable      bool
	DisplayName string
	HasAvatar   bool
	Bio         string
	FirstName   string
	LastName    string
	Location    string
}

// handleWhoami projects the own profile and detail logs for the local
// identity.
func (d *Daemon) handleWhoami(id core.Identity) (whoamiInfo, error) {
	out := whoamiInfo{Identity: id, Stable: true}
	d.mu.Lock()
	if d.tcListener != nil {
		out.Token = string(d.tcListener.Addr())
	}
	if d.ephemeralKey {
		out.Stable = false
	}
	d.mu.Unlock()
	profEvents, err := d.store.OwnEvents(core.ProfileLog)
	if err != nil {
		return out, err
	}
	if prof := core.NewLog(profEvents).Profile(); prof != nil {
		out.DisplayName = prof.DisplayName
		out.HasAvatar = prof.AvatarHash != nil && !prof.AvatarHash.IsZero()
	}
	detEvents, err := d.store.OwnEvents(core.DetailLog)
	if err == nil {
		if det := core.NewLog(detEvents).Detail(); det != nil {
			out.Bio = det.Bio
			out.FirstName = det.FirstName
			out.LastName = det.LastName
			out.Location = det.Location
		}
	}
	return out, nil
}

// handleFollows returns the identities this zen currently follows, derived
// by replaying its own ProfileLog Follow/Unfollow events (section 7.1).
func (d *Daemon) handleFollows() ([]*driftnodepb.Identity, error) {
	return d.followEntries(), nil
}

// handleFollowers returns the identities that follow this zen, derived from
// Follow events it has received targeting itself (section 9.3). This is the
// zen's own observed follower set, not a global truth.
func (d *Daemon) handleFollowers() ([]*driftnodepb.Identity, error) {
	return d.followerEntries(), nil
}

// handleProfile signs a Profile event (public: zen name) and appends it to
// the local ProfileLog. The ProfileLog is what the crawler fetches and caches
// durably, so only public-facing fields belong here.
func (d *Daemon) handleProfile(name string, kp *core.KeyPair) (core.EventID, error) {
	_, id, err := d.store.SignAndAppend(kp, core.ProfileLog, core.Event{
		Kind:    core.KindProfile,
		Profile: &core.Profile{DisplayName: name},
	})
	if err != nil {
		return core.EventID{}, err
	}
	d.invalidateNames()
	d.touchSignAt()
	d.triggerCrawlNow()
	return id, nil
}

// handleDetail signs a Detail event (bio, first/last name, location) and
// appends it to the local DetailLog. The DetailLog is never crawled and never
// cached durably by peers; it is fetched on demand for display and
// discarded. The owner's own DetailLog is backup-critical.
func (d *Daemon) handleDetail(bio, firstName, lastName, location string, kp *core.KeyPair) (core.EventID, error) {
	_, id, err := d.store.SignAndAppend(kp, core.DetailLog, core.Event{
		Kind:   core.KindDetail,
		Detail: &core.Detail{Bio: bio, FirstName: firstName, LastName: lastName, Location: location},
	})
	if err != nil {
		return core.EventID{}, err
	}
	d.touchSignAt()
	d.triggerCrawlNow()
	return id, nil
}

// handleFollow is the single "add a zen" gesture: it records a Follow event
// for the target identity. The target may be a driftnode identity/pubkey or
// a tailcat token. When given a token, it dials the zen, learns the identity
// from the session handshake, writes the Follow event, and binds the token
// to that identity in the routing table. When given a pubkey, it does the
// same when a token for that identity is already known (the common case:
// the zen was discovered via exchange), so the follow connects at once.
// Only when no token is known yet does it fall back to writing the Follow
// event and letting the sync loop resolve the token asynchronously.
func (d *Daemon) handleFollow(targetStr string, kp *core.KeyPair) (core.Identity, error) {
	// Identities and raw base32 pubkeys can be parsed unambiguously, so
	// try that first; anything else is treated as a token.
	if target, err := core.ResolvePubkey(targetStr); err == nil {
		return d.followByPubkey(target, kp)
	}
	return d.followByToken(targetStr, kp)
}

// followByPubkey writes a Follow event for the given pubkey. When a token is
// already known for that identity (the zen was discovered via exchange), it
// dials, binds the token to the identity in the routing table, and marks the
// zen connected before returning. When no token is known, it writes the
// Follow event and triggers a sync round so the sync loop can resolve the
// token asynchronously.
func (d *Daemon) followByPubkey(target [32]byte, kp *core.KeyPair) (core.Identity, error) {
	identity := core.IdentityFromPubkey(ed25519.PublicKey(target[:]))
	if tok, ok := d.tokenForIdentity(identity); ok {
		return d.followByToken(tok, kp)
	}
	return d.writeFollow(target, kp)
}

// followByToken dials a tailcat token, runs the handshake to learn the zen's
// identity, writes a Follow event for it, and binds the token to that
// identity in the routing table.
func (d *Daemon) followByToken(token string, kp *core.KeyPair) (core.Identity, error) {
	d.mu.Lock()
	if p, ok := d.zens[token]; ok {
		// Preserve identity/name learned via exchange; just update status.
		p.Status = "connecting"
	} else {
		d.zens[token] = &zenInfo{ID: token, Kind: "native_zen", Status: "connecting"}
	}
	transport := d.transport
	d.mu.Unlock()
	if transport == nil {
		return "", errors.New("no transport configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := transport.Dial(ctx, token)
	if err != nil {
		d.setZenStatus(token, "error")
		return "", fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	d.setZenStatus(token, "connected")
	var zenID core.Identity
	if _, err := d.runSession(conn, true, &zenID); err != nil {
		d.logger.Warn("follow: session failed", "token", token, "err", err)
		return "", fmt.Errorf("session: %w", err)
	}
	if zenID == "" {
		return "", errors.New("session: zen did not authenticate")
	}
	pub, err := zenID.PubkeyBytes()
	if err != nil {
		return "", fmt.Errorf("zen identity: %w", err)
	}
	if err := d.store.PutRouting(zenID, token); err != nil {
		return "", fmt.Errorf("routing: %w", err)
	}
	if err := d.learnToken(token); err != nil {
		d.logger.Warn("learn token failed", "token", token, "err", err)
	}
	return d.writeFollow([32]byte(pub), kp)
}

// writeFollow signs a Follow event for the given target pubkey and records the
// local follow edge. It does not dial; callers that can resolve a token
// should dial via followByToken so the routing is bound immediately.
func (d *Daemon) writeFollow(target [32]byte, kp *core.KeyPair) (core.Identity, error) {
	_, _, id, err := d.store.Follow(kp, target)
	if err != nil {
		return "", err
	}
	d.touchSignAt()
	d.triggerCrawlNow()
	d.refreshSplit()
	name, _ := d.store.DisplayName(id)
	d.emitFollowsAdd(&driftnodepb.Identity{Identity: id.String(), Name: name})
	return id, nil
}

// handleUnfollow signs an Unfollow event, removes the routing binding for
// the target, and drops the zen from the zens map. This is the single
// "remove a zen" gesture: unfollowing stops the daemon from dialing the
// identity on future sync rounds. The shared store.Unfollow clears the local
// follow edge and any pin so the clear-on-unfollow invariant is defined once.
func (d *Daemon) handleUnfollow(targetStr string, kp *core.KeyPair) (core.Identity, error) {
	target, err := core.ResolvePubkey(targetStr)
	if err != nil {
		return "", fmt.Errorf("resolve target: %w", err)
	}
	_, _, id, err := d.store.Unfollow(kp, target)
	if err != nil {
		return "", err
	}
	token, hadToken, _ := d.store.Routing(id)
	if err := d.store.DeleteRouting(id); err != nil {
		d.logger.Warn("unfollow: delete routing", "zen", id, "err", err)
	}
	if hadToken {
		d.removeZen(token)
	} else {
		d.removeZen(string(id))
	}
	d.emitFollowsRemove(id.String())
	d.touchSignAt()
	d.triggerCrawlNow()
	d.refreshSplit()
	return id, nil
}

// touchSignAt records that a signing call just happened, for idle-lock
// expiry, and reaps an expired key when idle-lock is configured.
func (d *Daemon) touchSignAt() {
	d.mu.Lock()
	d.lastSignAt = time.Now()
	d.mu.Unlock()
}
