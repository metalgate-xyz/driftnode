//go:build ignore

package main

import (
	"encoding/hex"
	"fmt"
	"os"

	"driftnode/internal/core"

	bolt "go.etcd.io/bbolt"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: dumpdb <db>")
		os.Exit(1)
	}
	db, err := bolt.Open(os.Args[1], 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer db.Close()

	ownID, ok, _ := readMetaIdentity(db)
	fmt.Printf("== own identity: %q (present=%v) ==\n\n", string(ownID), ok)

	fmt.Println("== own/post ==", )
	db.View(func(tx *bolt.Tx) error {
		ownBucket := tx.Bucket([]byte("own"))
		if ownBucket == nil {
			fmt.Println("(no own bucket)")
			return nil
		}
		postBucket := ownBucket.Bucket([]byte("post"))
		if postBucket == nil {
			fmt.Println("(no own/post bucket)")
			return nil
		}
		postBucket.ForEach(func(k, v []byte) error {
			var se core.SignedEvent
			if err := core.CanonicalDecode(v, &se); err != nil {
				fmt.Printf("  %x  <decode err: %v>\n", k, err)
				return nil
			}
			id, _ := se.ID()
			text := ""
			if se.Event.Post != nil {
				text = se.Event.Post.Text
			}
			fmt.Printf("  id=%s  log=%s kind=%d seq=%d ts=%d  author=%s  text=%q\n",
				id.String(), se.Event.Log, se.Event.Kind, se.Event.Sequence, se.Event.Timestamp, se.Author.String(), text)
			return nil
		})
		return nil
	})

	fmt.Println("\n== follows/<author> (PostLog events only) ==")
	db.View(func(tx *bolt.Tx) error {
		fb := tx.Bucket([]byte("follows"))
		if fb == nil {
			fmt.Println("(no follows bucket)")
			return nil
		}
		fb.ForEach(func(k, v []byte) error {
			authorBucket := fb.Bucket(k)
			if authorBucket == nil {
				return nil
			}
			fmt.Printf("author bucket: %q\n", string(k))
			authorBucket.ForEach(func(ek, ev []byte) error {
				var se core.SignedEvent
				if err := core.CanonicalDecode(ev, &se); err != nil {
					fmt.Printf("  %x  <decode err: %v>\n", ek, err)
					return nil
				}
				if se.Event.Log != core.PostLog {
					return nil
				}
				id, _ := se.ID()
				text := ""
				if se.Event.Post != nil {
					text = se.Event.Post.Text
				}
				fmt.Printf("  id=%s  log=%s kind=%d seq=%d ts=%d  author=%s  text=%q\n",
					id.String(), se.Event.Log, se.Event.Kind, se.Event.Sequence, se.Event.Timestamp, se.Author.String(), text)
				return nil
			})
			return nil
		})
		return nil
	})

	fmt.Println("\n== follows bucket key count per author (all logs) ==")
	db.View(func(tx *bolt.Tx) error {
		fb := tx.Bucket([]byte("follows"))
		if fb == nil {
			return nil
		}
		fb.ForEach(func(k, v []byte) error {
			authorBucket := fb.Bucket(k)
			if authorBucket == nil {
				return nil
			}
			n := 0
			profN := 0
			postN := 0
			detailN := 0
			authorBucket.ForEach(func(ek, ev []byte) error {
				n++
				var se core.SignedEvent
				if err := core.CanonicalDecode(ev, &se); err == nil {
					if se.Event.Log == core.PostLog {
						postN++
					} else if se.Event.Log == core.ProfileLog {
						profN++
					} else if se.Event.Log == core.DetailLog {
						detailN++
					}
				}
				return nil
			})
			fmt.Printf("  %q  total=%d  profile=%d  post=%d  detail=%d\n", string(k), n, profN, postN, detailN)
			return nil
		})
		return nil
	})
}

func readMetaIdentity(db *bolt.DB) (core.Identity, bool, error) {
	var id []byte
	err := db.View(func(tx *bolt.Tx) error {
		mb := tx.Bucket([]byte("meta"))
		if mb == nil {
			return nil
		}
		id = mb.Get([]byte("identity"))
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

var _ = hex.EncodeToString
