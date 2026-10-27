package cli

import (
	"fmt"
	"os"

	"driftnode/internal/core"

	"github.com/spf13/cobra"
)

func keyCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "key",
		Short: "Export or import the encrypted keypair",
	}
	addDBFlagPersistent(c)
	c.AddCommand(keyExportCmd(), keyImportCmd())
	return c
}

func keyExportCmd() *cobra.Command {
	var outPath string
	c := &cobra.Command{
		Use:   "export",
		Short: "Export the encrypted keypair to a file",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			ek, err := s.EncryptedKey()
			if err != nil {
				return fmt.Errorf("read key: %w", err)
			}
			b, err := core.CanonicalEncode(ek)
			if err != nil {
				return fmt.Errorf("encode: %w", err)
			}
			if outPath == "" {
				outPath = "driftnode-key.cbor"
			}
			if err := os.WriteFile(outPath, b, 0o600); err != nil {
				return fmt.Errorf("write: %w", err)
			}
			cmd.Printf("exported encrypted key to %s\n", outPath)
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&outPath, "out", "o", "", "output path (default: driftnode-key.cbor)")
	return c
}

func keyImportCmd() *cobra.Command {
	var passphrase string
	c := &cobra.Command{
		Use:   "import <path>",
		Short: "Import an encrypted keypair from a file, replacing the current one",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if passphrase == "" {
				return fmt.Errorf("--passphrase is required (needed to derive and store the identity)")
			}
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			b, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("read: %w", err)
			}
			var ek core.EncryptedKey
			if err := core.CanonicalDecode(b, &ek); err != nil {
				return fmt.Errorf("decode: %w", err)
			}
			if err := s.PutEncryptedKey(&ek); err != nil {
				return fmt.Errorf("store key: %w", err)
			}
			// Decrypt to derive the public key and store the identity.
			enc := core.DefaultKeyEncryption()
			priv, err := enc.Decrypt(&ek, []byte(passphrase))
			if err != nil {
				return fmt.Errorf("decrypt key: %w", err)
			}
			kp, err := core.KeyPairFromBytes(priv)
			if err != nil {
				return fmt.Errorf("reconstruct keypair: %w", err)
			}
			if err := s.SetIdentity(kp.Identity()); err != nil {
				return fmt.Errorf("store identity: %w", err)
			}
			cmd.Printf("imported key for %s\n", kp.Identity())
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to decrypt the key and derive the identity")
	return c
}

func backupCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "backup",
		Short: "Export or import a single-file account backup",
	}
	addDBFlagPersistent(c)
	c.AddCommand(backupExportCmd(), backupImportCmd())
	return c
}

func backupExportCmd() *cobra.Command {
	var outPath string
	var passphrase string
	c := &cobra.Command{
		Use:   "export",
		Short: "Write the single-file CBOR backup (Section 8)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			bd, err := s.ExportBackup()
			if err != nil {
				return fmt.Errorf("export: %w", err)
			}
			// The tailcat transport key is stored raw in the local store; in
			// the backup file it is sealed under the same passphrase as the
			// identity key, so the address token survives a device restore.
			tk, hasTK, err := s.TransportKey()
			if err != nil {
				return fmt.Errorf("read transport key: %w", err)
			}
			if hasTK {
				if passphrase == "" {
					return fmt.Errorf("--passphrase is required to encrypt the transport key into the backup")
				}
				enc, err := core.DefaultKeyEncryption().EncryptBytes(tk, []byte(passphrase))
				if err != nil {
					return fmt.Errorf("encrypt transport key: %w", err)
				}
				bd.TransportKey = enc
			}
			b, err := core.CanonicalEncode(bd)
			if err != nil {
				return fmt.Errorf("encode: %w", err)
			}
			if outPath == "" {
				outPath = "driftnode-backup.cbor"
			}
			if err := os.WriteFile(outPath, b, 0o600); err != nil {
				return fmt.Errorf("write: %w", err)
			}
			cmd.Printf("exported backup to %s\n", outPath)
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&outPath, "out", "o", "", "output path (default: driftnode-backup.cbor)")
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to encrypt the transport key into the backup")
	return c
}

func backupImportCmd() *cobra.Command {
	var passphrase string
	c := &cobra.Command{
		Use:   "import <path>",
		Short: "Restore or merge from a backup file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := openStoreAt(dbPath)
			if err != nil {
				return err
			}
			defer s.Close()
			b, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("read: %w", err)
			}
			var bd core.BackupData
			if err := core.CanonicalDecode(b, &bd); err != nil {
				return fmt.Errorf("decode: %w", err)
			}
			if passphrase == "" {
				return fmt.Errorf("--passphrase is required to restore the account keypair")
			}
			if err := s.ImportBackup(&bd, passphrase); err != nil {
				return fmt.Errorf("import: %w", err)
			}
			// Restore the transport key so the address token survives the
			// restore; it is sealed under the same passphrase as the key.
			if bd.TransportKey != nil {
				if passphrase == "" {
					return fmt.Errorf("--passphrase is required to decrypt the transport key")
				}
				tk, err := core.DefaultKeyEncryption().DecryptBytes(bd.TransportKey, []byte(passphrase))
				if err != nil {
					return fmt.Errorf("decrypt transport key: %w", err)
				}
				if err := s.PutTransportKey(tk); err != nil {
					return fmt.Errorf("restore transport key: %w", err)
				}
			}
			cmd.Println("imported backup")
			return nil
		},
	}
	addDBFlag(c)
	c.Flags().StringVarP(&passphrase, "passphrase", "p", "", "passphrase to decrypt the account keypair and transport key from the backup")
	return c
}
