// Command kv is an interactive shell for a storage.DB data file.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/p0tpan/kvstore/storage"
)

func main() {
	path := flag.String("db", "data.kv", "path to the data file")
	sync := flag.Bool("sync", false, "fsync after every write")
	flag.Parse()

	db, err := storage.Open(*path, storage.Options{SyncWrites: *sync})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Printf("opened %s (%d keys). commands: set <k> <v>, get <k>, del <k>, keys, exit\n", *path, db.Len())
	sc := bufio.NewScanner(os.Stdin)
	for fmt.Print("> "); sc.Scan(); fmt.Print("> ") {
		args := strings.SplitN(strings.TrimSpace(sc.Text()), " ", 3)
		switch args[0] {
		case "":
		case "set":
			if len(args) != 3 {
				fmt.Println("usage: set <key> <value>")
				continue
			}
			report(db.Put([]byte(args[1]), []byte(args[2])))
		case "get":
			if len(args) != 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			v, err := db.Get([]byte(args[1]))
			if errors.Is(err, storage.ErrNotFound) {
				fmt.Println("(not found)")
			} else if err != nil {
				fmt.Println("error:", err)
			} else {
				fmt.Println(string(v))
			}
		case "del":
			if len(args) != 2 {
				fmt.Println("usage: del <key>")
				continue
			}
			report(db.Delete([]byte(args[1])))
		case "keys":
			for _, k := range db.Keys() {
				fmt.Println(k)
			}
		case "exit", "quit":
			return
		default:
			fmt.Println("unknown command:", args[0])
		}
	}
}

func report(err error) {
	if err != nil {
		fmt.Println("error:", err)
	} else {
		fmt.Println("OK")
	}
}
