package core

// BackupData is the account-critical state serialized into a single backup
// file (§8): the encrypted keypair, the user's own Profile log and PostLog,
// their local petname map, and the identities they have confirmed
// out-of-band. Followed accounts' synced PostLogs and crawled Profile logs
// are re-fetchable cache, not backup-critical, and are excluded to keep the
// file small.
type BackupData struct {
	Key      *EncryptedKey `cbor:"k"`
	OwnLogs  []SignedEvent `cbor:"o"`           // own Profile log + PostLog, set union on import
	Petnames []Petname     `cbor:"p,omitempty"` // local nicknames for followed identities
	Verified []Identity     `cbor:"v,omitempty"` // out-of-band confirmed identities (§7)
}

// Petname is a local nickname for a followed identity, stored in the user's
// own Profile log (§9.4). It is metadata about the user's own graph, not
// content.
type Petname struct {
	TargetPubkey [32]byte `cbor:"t"`
	Name         string   `cbor:"n"`
}
