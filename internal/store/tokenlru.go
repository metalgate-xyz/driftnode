package store

import (
	bolt "go.etcd.io/bbolt"
)

// The token cache is the discovered-token LRU over the generic boltLRU,
// second specialization next to the crawl cache: keyed on last mention,
// refreshed on successful sync, capped at maxTokenCache. The keep
// predicate holds two classes of tokens: routing-bound tokens (a followed
// identity is bound to the token in bucketRouting) and bootstrap tokens
// (loaded from the bootstrap file into bucketBootstrapTk). There is no
// on-evict side effect: a token has no payload, the LRU entry pair is
// the token.

// maxTokenCacheDefault is the token LRU cap, the same source constant as
// the crawl cache cap. Tokens are offered on every session, so an evicted
// token is relearned on the next exchange with a holder that still has
// it hot: eviction is self-healing while at least one holder mentions it.
const maxTokenCacheDefault = 10000

// maxTokenCache is the active token LRU cap. A variable so tests can
// exercise the eviction boundary; production always runs at
// maxTokenCacheDefault.
var maxTokenCache = maxTokenCacheDefault

// tokenLRU is the discovered-token LRU specialization.
var tokenLRU = newBoltLRU(bucketTokenLRU, bucketTokenTS)

// tokenKeep reports whether a token must survive any LRU pressure:
// routing-bound tokens reach followed identities, bootstrap tokens are
// the node's configured entry points into the network. The routing table
// is keyed by identity with the token as the value, so a token matches a
// binding by scanning the values (the table holds only followed
// identities, so the scan is small).
func tokenKeep(tx *bolt.Tx) func(id []byte) bool {
	routing := tx.Bucket(bucketRouting)
	boot := tx.Bucket(bucketBootstrapTk)
	return func(id []byte) bool {
		if boot.Get(id) != nil {
			return true
		}
		found := false
		_ = routing.ForEach(func(_, v []byte) error {
			if string(v) == string(id) {
				found = true
			}
			return nil
		})
		return found
	}
}

// LearnToken records a discovered zen token and refreshes its last-mention
// time, evicting the coldest tokens to cap within the same transaction.
// A token learned here becomes durable: it survives restarts and is
// offered on every zen exchange.
func (s *Store) LearnToken(token string, when int64) error {
	if token == "" {
		return nil
	}
	return s.LearnTokens([]string{token}, when)
}

// LearnTokens records a batch of discovered zen tokens in one transaction:
// each token is touched, then one eviction pass runs to cap. One commit per
// batch keeps a full zen-exchange offer cheap regardless of how many tokens
// it carries.
func (s *Store) LearnTokens(tokens []string, when int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, tok := range tokens {
			if tok == "" {
				continue
			}
			if err := tokenLRU.touch(tx, []byte(tok), when); err != nil {
				return err
			}
		}
		return tokenLRU.evictToCap(tx, maxTokenCache, when, tokenKeep(tx), nil)
	})
}

// KnownTokens returns every tracked token. Enumerated for zen exchange
// offers and for rehydrating the in-memory zen map on restart.
func (s *Store) KnownTokens() ([]string, error) {
	var out []string
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTokenTS).ForEach(func(k, _ []byte) error {
			out = append(out, string(k))
			return nil
		})
	})
	return out, err
}

// PutBootstrapTokens replaces the persisted bootstrap token set. Bootstrap
// tokens are held by the token LRU's keep predicate, so they survive any
// eviction pressure. The tokens themselves are also touched into the LRU
// so they are offered on exchanges immediately.
func (s *Store) PutBootstrapTokens(tokens []string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		boot := tx.Bucket(bucketBootstrapTk)
		if err := boot.ForEach(func(k, _ []byte) error { return boot.Delete(k) }); err != nil {
			return err
		}
		now := s.now64()
		for _, tok := range tokens {
			if tok == "" {
				continue
			}
			if err := boot.Put([]byte(tok), []byte{1}); err != nil {
				return err
			}
			if err := tokenLRU.touch(tx, []byte(tok), now); err != nil {
				return err
			}
		}
		return nil
	})
}
