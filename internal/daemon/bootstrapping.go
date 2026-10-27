package daemon

import (
	"crypto/ed25519"
	"driftnode/internal/bootstrap"
	"driftnode/internal/core"
	"time"
)

// dialBootstrapSeeds loads a bootstrap.yaml, verifies its signature, and
// dials each seed zen (section 6.1). Called asynchronously from Start.
func (d *Daemon) dialBootstrapSeeds(path string, verifyKey ed25519.PublicKey) {
	bf, err := bootstrap.Load(path)
	if err != nil {
		d.logger.Warn("bootstrap load failed", "err", err)
		return
	}
	if verifyKey != nil {
		if err := bf.Verify(verifyKey); err != nil {
			d.logger.Warn("bootstrap signature verification failed", "err", err)
			return
		}
		d.logger.Info("bootstrap verified", "seed_zens", len(bf.SeedZens))
	} else {
		d.logger.Info("bootstrap loaded (no verification key)", "seed_zens", len(bf.SeedZens))
	}
	// Persist the seed token set: bootstrap tokens are held by the
	// token LRU's keep predicate and survive any eviction pressure.
	seedTokens := make([]string, 0, len(bf.SeedZens))
	for _, sp := range bf.SeedZens {
		if sp.Token != "" {
			seedTokens = append(seedTokens, sp.Token)
		}
	}
	if err := d.store.PutBootstrapTokens(seedTokens); err != nil {
		d.logger.Warn("persist bootstrap tokens failed", "err", err)
	}
	// Seed the crawler from crawl_seeds (section 9.3). The crawler walks
	// the follow graph, fetching Profile logs only. Crawl seeds can be
	// loaded without the signing key; the fetches will fail until unlock.
	var seeds []core.Identity
	for _, s := range bf.CrawlSeeds {
		if id, err := core.ParseIdentity(s); err == nil {
			seeds = append(seeds, id)
		}
	}
	if len(seeds) > 0 {
		d.mu.Lock()
		if d.crawler != nil {
			d.crawler.Seed(seeds...)
		}
		d.mu.Unlock()
		d.logger.Info("crawl seeds loaded", "count", len(seeds))
	}

	d.mu.Lock()
	kp := d.unlocked
	d.mu.Unlock()
	if kp == nil {
		// Key is locked: defer the auto-follow until unlock. Leave
		// bootstrapDone false so handleUnlock retries dialBootstrapSeeds.
		d.logger.Warn("bootstrap: key locked; deferring seed auto-follow")
		return
	}
	if !d.bootstrapMu.TryLock() {
		d.logger.Warn("bootstrap: follow pass already running")
		return
	}
	defer d.bootstrapMu.Unlock()

	// Follow each seed. Skip check is "not followed yet", not "token
	// known": following one seed exchanges zen lists, which adds the
	// other seeds to the token cache, and skipping known tokens would
	// drop them from the follow pass.
	var pending []bootstrap.SeedZen
	for _, sp := range bf.SeedZens {
		if sp.Token != "" {
			pending = append(pending, sp)
		}
	}
	delay := d.bootstrapRetryDelay
	if delay == 0 {
		delay = bootstrapSeedRetryDelay
	}
	followed := 0
	for pass := 1; len(pending) > 0; pass++ {
		var failed []bootstrap.SeedZen
		for _, sp := range pending {
			d.upsertZen(sp.Token, sp.Kind, "connecting")
			if _, err := d.handleFollow(sp.Token, kp); err != nil {
				d.logger.Warn("bootstrap: follow seed failed",
					"token", sp.Token, "err", err, "pass", pass)
				failed = append(failed, sp)
				continue
			}
			followed++
		}
		pending = failed
		if len(pending) == 0 || pass >= bootstrapSeedAttempts {
			break
		}
		// A seed can be temporarily down; retry after a pause (two
		// retries, three passes total). The pass stops for good if the
		// daemon is stopping.
		select {
		case <-d.Done():
			return
		case <-time.After(delay):
		}
		d.mu.Lock()
		kp = d.unlocked
		d.mu.Unlock()
		if kp == nil {
			// Locked again (idle lock): let the next unlock retry.
			d.logger.Warn("bootstrap: key locked; deferring seed auto-follow")
			return
		}
	}
	for _, sp := range pending {
		d.logger.Warn("bootstrap: seed unreachable", "token", sp.Token)
	}
	d.logger.Info("bootstrap: seeds followed", "followed", followed, "unreachable", len(pending))

	// Mark bootstrap complete so handleUnlock doesn't retry it.
	d.mu.Lock()
	d.bootstrapDone = true
	d.mu.Unlock()
}
