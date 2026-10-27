package store

import (
	"errors"
	"fmt"

	"driftnode/internal/core"
)

// ExportBackup returns the account-critical state for a single-file backup
// (§8): the encrypted keypair and the user's own logs.
func (s *Store) ExportBackup() (*core.BackupData, error) {
	ek, err := s.EncryptedKey()
	if err != nil {
		return nil, err
	}
	events, err := s.AllOwnEvents()
	if err != nil {
		return nil, err
	}
	verified, err := s.VerifiedIdentities()
	if err != nil {
		return nil, err
	}
	pinned, err := s.PinnedIdentities()
	if err != nil {
		return nil, err
	}
	return &core.BackupData{Key: ek, OwnLogs: events, Verified: verified, Pinned: pinned}, nil
}

// ImportBackup restores a backup into the store. It is the account restore
// path: the encrypted keypair is decrypted with the passphrase and stored
// with its identity, own logs merge as a set union of signed events (§8),
// verified identities replace the local set, and the follow-state cache is
// rebuilt before the backup's pins are applied onto the rebuilt follow edges
// (a pin requires a follow). A backup whose decrypted keypair belongs to a
// different identity than the store's is refused outright: import restores
// THIS account, it never merges another identity's events into it.
// AppendOwnEvent dedups by event ID, so re-importing is idempotent.
func (s *Store) ImportBackup(bd *core.BackupData, passphrase string) error {
	if bd.Key == nil {
		return errors.New("backup missing key")
	}
	priv, err := core.DefaultKeyEncryption().Decrypt(bd.Key, []byte(passphrase))
	if err != nil {
		return fmt.Errorf("decrypt backup keypair: %w", err)
	}
	kp, err := core.KeyPairFromBytes(priv)
	if err != nil {
		return fmt.Errorf("reconstruct keypair: %w", err)
	}
	if id, ok, err := s.Identity(); err != nil {
		return err
	} else if ok && id != kp.Identity() {
		return fmt.Errorf("backup belongs to %s; this store is %s", kp.Identity(), id)
	}
	if err := s.PutKey(kp, bd.Key); err != nil {
		return fmt.Errorf("restore keypair: %w", err)
	}
	for _, se := range bd.OwnLogs {
		if err := s.AppendOwnEvent(se.Event.Log, &se); err != nil {
			return err
		}
	}
	if bd.Verified != nil {
		if err := s.PutVerifiedIdentities(bd.Verified); err != nil {
			return fmt.Errorf("restore verified: %w", err)
		}
	}
	if err := s.rebuildFollowState(); err != nil {
		return fmt.Errorf("rebuild follow state: %w", err)
	}
	if bd.Pinned != nil {
		if err := s.PutPinnedIdentities(bd.Pinned); err != nil {
			return fmt.Errorf("restore pinned: %w", err)
		}
	}
	return nil
}
