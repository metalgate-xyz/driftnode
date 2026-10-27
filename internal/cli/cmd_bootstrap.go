package cli

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"

	bootstrappkg "driftnode/internal/bootstrap"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func bootstrapCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "bootstrap",
		Short: "Manage the bootstrap file",
	}
	c.AddCommand(bootstrapKeygenCmd(), bootstrapSignCmd(), bootstrapVerifyCmd())
	return c
}

// bootstrapKeygenCmd generates an Ed25519 keypair for signing bootstrap
// files. The private key (64-byte raw, seed||pubkey) goes to --key-out; the
// base64 public key is printed to stdout for use with `verify --key` and
// `daemon --bootstrap-key`. With --key it instead derives the public key
// from an existing private key without generating or writing anything.
func bootstrapKeygenCmd() *cobra.Command {
	var keyOut, keyIn string
	c := &cobra.Command{
		Use:   "keygen",
		Short: "Generate an Ed25519 keypair, or derive the public key from an existing private key",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if keyIn != "" {
				keyData, err := os.ReadFile(keyIn)
				if err != nil {
					return fmt.Errorf("read key: %w", err)
				}
				priv := ed25519.PrivateKey(keyData)
				if len(priv) != ed25519.PrivateKeySize {
					return fmt.Errorf("key file: want %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
				}
				pub, ok := priv.Public().(ed25519.PublicKey)
				if !ok {
					return fmt.Errorf("invalid ed25519 private key")
				}
				cmd.Println(base64.StdEncoding.EncodeToString(pub))
				return nil
			}
			if keyOut == "" {
				return fmt.Errorf("--key-out is required (or use --key to derive a public key)")
			}
			pub, priv, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				return fmt.Errorf("generate key: %w", err)
			}
			if err := os.WriteFile(keyOut, priv, 0o600); err != nil {
				return fmt.Errorf("write private key: %w", err)
			}
			cmd.Println(base64.StdEncoding.EncodeToString(pub))
			return nil
		},
	}
	c.Flags().StringVar(&keyOut, "key-out", "", "path to write the 64-byte raw Ed25519 private key")
	c.Flags().StringVar(&keyIn, "key", "", "path to an existing private key (derive its public key instead of generating)")
	return c
}

func bootstrapSignCmd() *cobra.Command {
	var keyFile string
	c := &cobra.Command{
		Use:   "sign <file>",
		Short: "Sign a bootstrap.yaml with an Ed25519 private key file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bf, err := bootstrappkg.Load(args[0])
			if err != nil {
				return err
			}
			keyData, err := os.ReadFile(keyFile)
			if err != nil {
				return fmt.Errorf("read key: %w", err)
			}
			priv := ed25519.PrivateKey(keyData)
			if len(priv) != ed25519.PrivateKeySize {
				return fmt.Errorf("key file: want %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
			}
			if err := bf.Sign(priv); err != nil {
				return err
			}
			out, err := yaml.Marshal(bf)
			if err != nil {
				return err
			}
			if err := os.WriteFile(args[0], out, 0o600); err != nil {
				return err
			}
			cmd.Printf("signed %s\n", args[0])
			return nil
		},
	}
	c.Flags().StringVar(&keyFile, "key", "", "path to a 64-byte raw Ed25519 private key file")
	_ = c.MarkFlagRequired("key")
	return c
}

func bootstrapVerifyCmd() *cobra.Command {
	var keyFile string
	c := &cobra.Command{
		Use:   "verify <file>",
		Short: "Verify a bootstrap.yaml's signature",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bf, err := bootstrappkg.Load(args[0])
			if err != nil {
				return err
			}
			cmd.Printf("version: %d\n", bf.Version)
			cmd.Printf("seed_relays: %d\n", len(bf.SeedRelays))
			cmd.Printf("derp_relays: %d\n", len(bf.DERPRelays))
			cmd.Printf("seed_zens: %d\n", len(bf.SeedZens))
			cmd.Printf("crawl_seeds: %d\n", len(bf.CrawlSeeds))
			if bf.Signature == "" {
				cmd.Println("signature: (none)")
				return nil
			}
			if keyFile == "" {
				cmd.Println("signature: present (no --key provided to verify against)")
				return nil
			}
			pub, err := decodeBootstrapKey(keyFile)
			if err != nil {
				return err
			}
			if err := bf.Verify(pub); err != nil {
				return fmt.Errorf("signature verification failed: %w", err)
			}
			cmd.Println("signature: verified")
			return nil
		},
	}
	c.Flags().StringVar(&keyFile, "key", "", "path to a file containing the base64 Ed25519 public key to verify against")
	return c
}

func relayCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "relay",
		Short: "Manage the relay/bootstrap role",
	}
	c.AddCommand(relayStatusCmd(), relayEnableCmd(), relayDisableCmd())
	return c
}

func relayStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show reachability check result and relay role status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.Println("relay role: not advertised")
			cmd.Println("(use 'relay enable' to advertise as a bootstrap candidate)")
			return nil
		},
	}
}

func relayEnableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "enable",
		Short: "Opt in to advertising this node as a relay/bootstrap candidate",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.Println("relay enable: not yet implemented (requires DHT integration)")
			return nil
		},
	}
}

func relayDisableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disable",
		Short: "Opt out of advertising this node as a relay/bootstrap candidate",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.Println("relay disable: not yet implemented")
			return nil
		},
	}
}
