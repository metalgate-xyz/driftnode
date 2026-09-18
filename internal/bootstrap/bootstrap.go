// Package bootstrap parses and verifies the signed bootstrap.yaml file that
// a fresh install uses to find its first zens (section 6.1). The file is a
// static, signed YAML document: verified content, not trusted location.
package bootstrap

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// File is the structure of bootstrap.yaml.
type File struct {
	Version    int         `yaml:"version"`
	Signature  string      `yaml:"signature"`
	SeedRelays []SeedRelay `yaml:"seed_relays"`
	DERPRelays []DERPRelay `yaml:"derp_relays"`
	SeedZens   []SeedZen   `yaml:"seed_zens"`
	CrawlSeeds []string    `yaml:"crawl_seeds"`
}

// SeedRelay is a WebRTC signaling relay URL.
type SeedRelay struct {
	URL  string `yaml:"url"`
	Kind string `yaml:"kind"`
}

// DERPRelay is a maintainer-run DERP relay.
type DERPRelay struct {
	Region string `yaml:"region"`
	URL    string `yaml:"url"`
}

// SeedZen is a native zen's tailcat token.
type SeedZen struct {
	Token string `yaml:"token"`
	Kind  string `yaml:"kind"`
}

// signedContent is the File without the Signature field, used to compute
// and verify the signature.
type signedContent struct {
	Version    int         `yaml:"version"`
	SeedRelays []SeedRelay `yaml:"seed_relays"`
	DERPRelays []DERPRelay `yaml:"derp_relays"`
	SeedZens   []SeedZen   `yaml:"seed_zens"`
	CrawlSeeds []string    `yaml:"crawl_seeds"`
}

func (f *File) content() signedContent {
	return signedContent{
		Version:    f.Version,
		SeedRelays: f.SeedRelays,
		DERPRelays: f.DERPRelays,
		SeedZens:   f.SeedZens,
		CrawlSeeds: f.CrawlSeeds,
	}
}

// MarshalContent returns the canonical YAML of the file content (without the
// signature), which is what the signature covers.
func (f *File) MarshalContent() ([]byte, error) {
	return yaml.Marshal(f.content())
}

// Sign computes the Ed25519 signature over the canonical content and sets
// the Signature field.
func (f *File) Sign(priv ed25519.PrivateKey) error {
	content, err := f.MarshalContent()
	if err != nil {
		return fmt.Errorf("marshal content: %w", err)
	}
	sig := ed25519.Sign(priv, content)
	f.Signature = "ed25519:" + encodeHex(sig)
	return nil
}

// Verify checks the Ed25519 signature against the given public key. The
// signature covers the canonical YAML of the content fields.
func (f *File) Verify(pub ed25519.PublicKey) error {
	sig, err := parseSig(f.Signature)
	if err != nil {
		return err
	}
	content, err := f.MarshalContent()
	if err != nil {
		return fmt.Errorf("marshal content: %w", err)
	}
	if !ed25519.Verify(pub, content, sig) {
		return errors.New("bootstrap signature does not verify")
	}
	return nil
}

// Load reads and parses a bootstrap file from path. It does NOT verify the
// signature; call Verify separately.
func Load(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read bootstrap: %w", err)
	}
	return Parse(data)
}

// Parse parses bootstrap YAML bytes.
func Parse(data []byte) (*File, error) {
	var f File
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse bootstrap yaml: %w", err)
	}
	return &f, nil
}

// parseSig decodes an "ed25519:<hex>" signature string.
func parseSig(s string) ([]byte, error) {
	const prefix = "ed25519:"
	if len(s) < len(prefix) || s[:len(prefix)] != prefix {
		return nil, errors.New("signature missing ed25519: prefix")
	}
	return decodeHex(s[len(prefix):])
}

func encodeHex(b []byte) string {
	const hexChars = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hexChars[v>>4]
		out[i*2+1] = hexChars[v&0xf]
	}
	return string(out)
}

func decodeHex(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, errors.New("odd-length hex")
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(s); i += 2 {
		hi, ok1 := hexVal(s[i])
		lo, ok2 := hexVal(s[i+1])
		if !ok1 || !ok2 {
			return nil, errors.New("invalid hex character")
		}
		out[i/2] = hi<<4 | lo
	}
	return out, nil
}

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}
