//go:build ignore

package main

import (
	"encoding/hex"
	"fmt"
	"os"

	bolt "go.etcd.io/bbolt"
)

func main() {
	if len(os.Args) < 2 {
		os.Exit(1)
	}
	db, _ := bolt.Open(os.Args[1], 0o600, &bolt.Options{ReadOnly: true})
	defer db.Close()
	db.View(func(tx *bolt.Tx) error {
		mb := tx.Bucket([]byte("meta"))
		raw := mb.Get([]byte("identity"))
		fmt.Printf("meta identity bytes (%d): %s\n", len(raw), hex.EncodeToString(raw))
		fmt.Printf("meta identity string: %q\n", string(raw))
		fmt.Printf("meta identity hex per byte: ")
		for _, b := range raw {
			fmt.Printf("%02x ", b)
		}
		fmt.Println()
		return nil
	})
	// Also dump an event author from own/post
	db.View(func(tx *bolt.Tx) error {
		ownBucket := tx.Bucket([]byte("own"))
		if ownBucket == nil {
			return nil
		}
		postBucket := ownBucket.Bucket([]byte("post"))
		if postBucket == nil {
			return nil
		}
		postBucket.ForEach(func(k, v []byte) error {
			fmt.Printf("\nown/post key bytes (%d): %s\n", len(k), hex.EncodeToString(k))
			return nil
		})
		return nil
	})
}
