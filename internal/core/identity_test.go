package core

import (
	"bytes"
	"strings"
	"testing"
)

func TestIdentityRoundTrip(t *testing.T) {
	kp, err := NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	id := kp.Identity()
	if !strings.HasPrefix(string(id), IdentityPrefix) {
		t.Fatalf("identity %q missing prefix %q", id, IdentityPrefix)
	}
	// The address must not carry routing detail beyond the identity prefix.
	// It should be exactly "driftnode:" + base32, with no host, port, or path.
	suffix := strings.TrimPrefix(string(id), IdentityPrefix)
	if strings.ContainsAny(suffix, ":/") {
		t.Fatalf("identity %q contains routing detail", id)
	}

	pub, err := id.PubkeyBytes()
	if err != nil {
		t.Fatalf("PubkeyBytes: %v", err)
	}
	if !bytes.Equal(pub, kp.Public) {
		t.Fatal("round-trip pubkey mismatch")
	}
}

func TestIdentityRejectsBadInput(t *testing.T) {
	cases := []string{
		"",
		"driftnode:",
		"notdriftnode:abc",
		"driftnode:!!!",
	}
	for _, c := range cases {
		if _, err := Identity(c).PubkeyBytes(); err == nil {
			t.Fatalf("expected error for %q", c)
		}
	}
}

func TestKeyPairFromBytes(t *testing.T) {
	kp, err := NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	kp2, err := KeyPairFromBytes(kp.Private)
	if err != nil {
		t.Fatalf("KeyPairFromBytes: %v", err)
	}
	if kp2.Identity() != kp.Identity() {
		t.Fatal("reconstructed identity differs")
	}
}

func TestKeyEncryptionRoundTrip(t *testing.T) {
	kp, err := NewKeyPair()
	if err != nil {
		t.Fatalf("NewKeyPair: %v", err)
	}
	enc := DefaultKeyEncryption()
	ek, err := enc.Encrypt(kp.Private, []byte("correct horse battery staple"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	priv, err := enc.Decrypt(ek, []byte("correct horse battery staple"))
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	kp2, err := KeyPairFromBytes(priv)
	if err != nil {
		t.Fatalf("KeyPairFromBytes: %v", err)
	}
	if kp2.Identity() != kp.Identity() {
		t.Fatal("decrypted key differs")
	}
}

func TestKeyEncryptionWrongPassphrase(t *testing.T) {
	kp, _ := NewKeyPair()
	enc := DefaultKeyEncryption()
	ek, err := enc.Encrypt(kp.Private, []byte("right"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if _, err := enc.Decrypt(ek, []byte("wrong")); err == nil {
		t.Fatal("expected error for wrong passphrase")
	}
}

func TestKeyEncryptionEmptyPassphrase(t *testing.T) {
	kp, _ := NewKeyPair()
	enc := DefaultKeyEncryption()
	if _, err := enc.Encrypt(kp.Private, nil); err == nil {
		t.Fatal("expected error for empty passphrase")
	}
}

// EncryptedKey must round-trip through canonical CBOR, since it is stored and
// written to backup files.
func TestEncryptedKeySerialization(t *testing.T) {
	kp, _ := NewKeyPair()
	enc := DefaultKeyEncryption()
	ek, _ := enc.Encrypt(kp.Private, []byte("pw"))
	canon, err := CanonicalEncode(ek)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var ek2 EncryptedKey
	if err := CanonicalDecode(canon, &ek2); err != nil {
		t.Fatalf("decode: %v", err)
	}
	priv, err := enc.Decrypt(&ek2, []byte("pw"))
	if err != nil {
		t.Fatalf("Decrypt after serialize: %v", err)
	}
	if !bytes.Equal(priv, kp.Private) {
		t.Fatal("serialized+decrypted key differs")
	}
}
