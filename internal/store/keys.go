package store

import (
	"errors"
	"fmt"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

// InitIdentity stores the encrypted keypair and the derived identity string.
// This is what `driftnode init` calls after generating a keypair.
func (s *Store) InitIdentity(kp *core.KeyPair, enc *core.EncryptedKey) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		keyBucket := tx.Bucket(bucketKey)
		if existing := keyBucket.Get([]byte("key")); existing != nil {
			return errors.New("identity already initialized; use `key import` to replace")
		}
		encBytes, err := core.CanonicalEncode(enc)
		if err != nil {
			return fmt.Errorf("encode key: %w", err)
		}
		if err := keyBucket.Put([]byte("key"), encBytes); err != nil {
			return fmt.Errorf("put key: %w", err)
		}
		return tx.Bucket(bucketMeta).Put(metaKeyIdentity, []byte(kp.Identity().String()))
	})
}

// Identity returns the stored identity string, or false if not initialized.
func (s *Store) Identity() (core.Identity, bool, error) {
	var id []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		id = tx.Bucket(bucketMeta).Get(metaKeyIdentity)
		return nil
	})
	if err != nil {
		return "", false, err
	}
	if id == nil {
		return "", false, nil
	}
	return core.Identity(id), true, nil
}

// EncryptedKey returns the stored encrypted keypair.
func (s *Store) EncryptedKey() (*core.EncryptedKey, error) {
	var raw []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		raw = tx.Bucket(bucketKey).Get([]byte("key"))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, errors.New("no keypair stored; run `driftnode init`")
	}
	var ek core.EncryptedKey
	if err := core.CanonicalDecode(raw, &ek); err != nil {
		return nil, fmt.Errorf("decode key: %w", err)
	}
	return &ek, nil
}

// PutKey replaces the stored encrypted keypair (used by `key import`).
func (s *Store) PutKey(kp *core.KeyPair, enc *core.EncryptedKey) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		encBytes, err := core.CanonicalEncode(enc)
		if err != nil {
			return fmt.Errorf("encode key: %w", err)
		}
		if err := tx.Bucket(bucketKey).Put([]byte("key"), encBytes); err != nil {
			return err
		}
		return tx.Bucket(bucketMeta).Put(metaKeyIdentity, []byte(kp.Identity().String()))
	})
}

// PutEncryptedKey stores an encrypted keypair directly (used by `key import`),
// without the identity string (which can only be derived after decryption).
func (s *Store) PutEncryptedKey(enc *core.EncryptedKey) error {
	encBytes, err := core.CanonicalEncode(enc)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketKey).Put([]byte("key"), encBytes); err != nil {
			return err
		}
		// Clear the cached identity; it will be set when the key is next
		// unlocked and the keypair is known.
		return tx.Bucket(bucketMeta).Delete(metaKeyIdentity)
	})
}

// SetIdentity stores the identity string for the current keypair. Called by
// the CLI after unlocking an imported key.
func (s *Store) SetIdentity(id core.Identity) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put(metaKeyIdentity, []byte(id.String()))
	})
}

// PutTransportKey stores the tailcat private key bytes, so the node's address
// token stays stable across restarts (section 5.2). The bytes are the JSON
// serialization of tailcat.PrivateKey.
func (s *Store) PutTransportKey(keyBytes []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTransport).Put([]byte("tailcat"), keyBytes)
	})
}

// TransportKey returns the stored tailcat private key bytes, or false if no
// key has been persisted yet.
func (s *Store) TransportKey() ([]byte, bool, error) {
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		data = tx.Bucket(bucketTransport).Get([]byte("tailcat"))
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if data == nil {
		return nil, false, nil
	}
	return append([]byte(nil), data...), true, nil
}

// DeleteTransportKey removes the stored tailcat private key.
func (s *Store) DeleteTransportKey() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTransport).Delete([]byte("tailcat"))
	})
}

// VerifyIdentity records that the user has confirmed an identity
// out-of-band (compared the full driftnode:<pubkey> string through a trusted
// channel). Verified identities are backup-critical trust state: losing them
// would silently downgrade a confirmed peer to unverified on restore.
func (s *Store) VerifyIdentity(id core.Identity) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketVerified).Put([]byte(id), []byte{1})
	})
}

// UnverifyIdentity removes an out-of-band confirmation.
func (s *Store) UnverifyIdentity(id core.Identity) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketVerified).Delete([]byte(id))
	})
}

// IsVerified reports whether an identity was confirmed out-of-band.
func (s *Store) IsVerified(id core.Identity) (bool, error) {
	var present bool
	err := s.db.View(func(tx *bolt.Tx) error {
		present = tx.Bucket(bucketVerified).Get([]byte(id)) != nil
		return nil
	})
	return present, err
}

// VerifiedIdentities returns every identity the user has confirmed out-of-band,
// used for backup export and for the zens view.
func (s *Store) VerifiedIdentities() ([]core.Identity, error) {
	var out []core.Identity
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketVerified).ForEach(func(k, v []byte) error {
			out = append(out, core.Identity(k))
			return nil
		})
	})
	return out, err
}

// PutVerifiedIdentities replaces the verified set, used by backup import.
func (s *Store) PutVerifiedIdentities(ids []core.Identity) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketVerified)
		if err := b.ForEach(func(k, v []byte) error { return b.Delete(k) }); err != nil {
			return err
		}
		for _, id := range ids {
			if err := b.Put([]byte(id), []byte{1}); err != nil {
				return err
			}
		}
		return nil
	})
}
