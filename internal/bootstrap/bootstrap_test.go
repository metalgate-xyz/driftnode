package bootstrap

import (
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestSignAndVerify(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	f := &File{
		Version: 1,
		SeedPeers: []SeedPeer{
			{Token: "tcexample123", Kind: "native_peer"},
		},
		CrawlSeeds: []string{"driftnode:abc123"},
	}
	if err := f.Sign(priv); err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if f.Signature == "" {
		t.Fatal("Sign did not set signature")
	}
	if err := f.Verify(pub); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestVerifyRejectsTampering(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	f := &File{
		Version:    1,
		CrawlSeeds: []string{"driftnode:abc"},
	}
	f.Sign(priv)
	f.CrawlSeeds[0] = "driftnode:xyz"
	if err := f.Verify(pub); err == nil {
		t.Fatal("Verify accepted tampered file")
	}
}

func TestVerifyRejectsWrongKey(t *testing.T) {
	pub1, priv1, _ := ed25519.GenerateKey(rand.Reader)
	f := &File{Version: 1}
	if err := f.Sign(priv1); err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if f.Signature == "" {
		t.Fatal("Sign did not set signature")
	}
	if err := f.Verify(pub1); err != nil {
		t.Fatalf("Verify with correct key: %v", err)
	}
	pub2, _, _ := ed25519.GenerateKey(rand.Reader)
	if err := f.Verify(pub2); err == nil {
		t.Fatal("Verify accepted wrong key")
	}
}

func TestLoadAndVerify(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	f := &File{
		Version:    1,
		SeedPeers:  []SeedPeer{{Token: "tctest", Kind: "native_peer"}},
		CrawlSeeds: []string{"driftnode:seed1"},
	}
	f.Sign(priv)

	dir := t.TempDir()
	path := filepath.Join(dir, "bootstrap.yaml")
	yamlOut, _ := yamlMarshalWithSig(f)
	_ = os.WriteFile(path, yamlOut, 0o600)

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := loaded.Verify(pub); err != nil {
		t.Fatalf("Verify loaded: %v", err)
	}
	if loaded.Version != 1 {
		t.Fatalf("version: want 1, got %d", loaded.Version)
	}
}

func TestParse(t *testing.T) {
	yamlData := []byte(`
version: 1
signature: "ed25519:abcd"
seed_peers:
  - token: "tctest"
    kind: native_peer
crawl_seeds:
  - "driftnode:abc"
`)
	f, err := Parse(yamlData)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if f.Version != 1 {
		t.Fatalf("version: want 1, got %d", f.Version)
	}
	if len(f.SeedPeers) != 1 || f.SeedPeers[0].Token != "tctest" {
		t.Fatalf("seed peers: %+v", f.SeedPeers)
	}
	if len(f.CrawlSeeds) != 1 || f.CrawlSeeds[0] != "driftnode:abc" {
		t.Fatalf("crawl seeds: %+v", f.CrawlSeeds)
	}
}

// yamlMarshalWithSig marshals the File including the signature field.
func yamlMarshalWithSig(f *File) ([]byte, error) {
	return yaml.Marshal(f)
}
