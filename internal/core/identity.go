package core

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// IdentityPrefix is the human-readable prefix of every driftnode identity
// string.
const IdentityPrefix = "driftnode:"

// Identity is the self-certifying address of an account: driftnode:<pubkey>,
// where pubkey is the lowercase base32 encoding of the account's 32-byte
// Ed25519 public key. No host, relay, or routing detail is encoded in it,
// because routing changes while identity must not (§7).
type Identity string

// String returns the full driftnode:<pubkey> address.
func (i Identity) String() string { return string(i) }

// PubkeyBytes returns the raw 32-byte public key encoded in the identity.
func (i Identity) PubkeyBytes() (ed25519.PublicKey, error) {
	s := string(i)
	if !strings.HasPrefix(s, IdentityPrefix) {
		return nil, errors.New("identity missing driftnode: prefix")
	}
	return PubkeyFromBase32(s[len(IdentityPrefix):])
}

// IdentityFromPubkey constructs the driftnode:<pubkey> address for a raw public
// key.
func IdentityFromPubkey(pub ed25519.PublicKey) Identity {
	return Identity(IdentityPrefix + base32NoPadLower(pub))
}

// ParseIdentity parses a string as an identity, validating the prefix and the
// embedded public key.
func ParseIdentity(s string) (Identity, error) {
	if !strings.HasPrefix(s, IdentityPrefix) {
		return "", fmt.Errorf("identity %q missing %q prefix", s, IdentityPrefix)
	}
	id := Identity(s)
	if _, err := id.PubkeyBytes(); err != nil {
		return "", err
	}
	return id, nil
}

// KeyPair is an Ed25519 keypair. The public key is the identity; the private
// key signs events. It is the entire account: there is no recovery outside
// the backup file that holds it (§8).
type KeyPair struct {
	Public  ed25519.PublicKey
	Private ed25519.PrivateKey
}

// NewKeyPair generates a fresh keypair from a cryptographically secure source.
func NewKeyPair() (*KeyPair, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ed25519: %w", err)
	}
	return &KeyPair{Public: pub, Private: priv}, nil
}

// KeyPairFromBytes reconstructs a keypair from a raw 64-byte private key. The
// public key is derived from the private key.
func KeyPairFromBytes(priv ed25519.PrivateKey) (*KeyPair, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("private key: want %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
	}
	pub, ok := priv.Public().(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("invalid ed25519 private key")
	}
	return &KeyPair{Public: pub, Private: priv}, nil
}

// Identity returns the driftnode:<pubkey> address for this keypair.
func (k *KeyPair) Identity() Identity { return IdentityFromPubkey(k.Public) }

// EncryptedKey is the at-rest form of a private key, stored in the local
// store and written into a backup file (§8). Only the private key is
// encrypted; the public key is the identity and is stored alongside it so an
// identity can be displayed without prompting for the passphrase.
type EncryptedKey struct {
	Salt    []byte `cbor:"1"`
	Nonce   []byte `cbor:"2"`
	Cipher  []byte `cbor:"3"`
	Time    uint32 `cbor:"4"`
	Memory  uint32 `cbor:"5"`
	Threads uint8  `cbor:"6"`
}

// KeyEncryption applies passphrase-based encryption to a private key at rest,
// using argon2id as the KDF and AES-256-GCM as the AEAD (§4).
type KeyEncryption struct {
	time    uint32
	memory  uint32
	threads uint8
}

// DefaultKeyEncryption uses parameters calibrated for interactive unlock on a
// personal device.
func DefaultKeyEncryption() KeyEncryption {
	return KeyEncryption{
		time:    3,
		memory:  32 * 1024,
		threads: 4,
	}
}

// Encrypt derives a 32-byte key from the passphrase and a fresh salt via
// argon2id, then seals the private key with AES-256-GCM under a fresh nonce.
func (k KeyEncryption) Encrypt(private ed25519.PrivateKey, passphrase []byte) (*EncryptedKey, error) {
	if len(passphrase) == 0 {
		return nil, errors.New("passphrase required")
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("salt: %w", err)
	}
	dk := argon2.IDKey(passphrase, salt, k.time, k.memory, k.threads, 32)

	block, err := aes.NewCipher(dk)
	if err != nil {
		return nil, fmt.Errorf("aes: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("gcm: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("nonce: %w", err)
	}
	cipherText := gcm.Seal(nil, nonce, private, nil)
	return &EncryptedKey{
		Salt:    salt,
		Nonce:   nonce,
		Cipher:  cipherText,
		Time:    k.time,
		Memory:  k.memory,
		Threads: k.threads,
	}, nil
}

// Decrypt reverses Encrypt. A wrong passphrase produces a GCM authentication
// error.
func (k KeyEncryption) Decrypt(ek *EncryptedKey, passphrase []byte) (ed25519.PrivateKey, error) {
	dk := argon2.IDKey(passphrase, ek.Salt, ek.Time, ek.Memory, ek.Threads, 32)
	block, err := aes.NewCipher(dk)
	if err != nil {
		return nil, fmt.Errorf("aes: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("gcm: %w", err)
	}
	if len(ek.Nonce) != gcm.NonceSize() {
		return nil, errors.New("nonce size mismatch")
	}
	priv, err := gcm.Open(nil, ek.Nonce, ek.Cipher, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt (wrong passphrase?): %w", err)
	}
	if len(priv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("private key: want %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
	}
	return ed25519.PrivateKey(priv), nil
}
