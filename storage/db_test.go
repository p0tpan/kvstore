package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func openTemp(t *testing.T) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.kv")
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return db, path
}

func reopen(t *testing.T, db *DB, path string) *DB {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mustGet(t *testing.T, db *DB, key, want string) {
	t.Helper()
	got, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if string(got) != want {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}

func mustMiss(t *testing.T, db *DB, key string) {
	t.Helper()
	if _, err := db.Get([]byte(key)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%q) error = %v, want ErrNotFound", key, err)
	}
}

func TestPutGetDelete(t *testing.T) {
	db, _ := openTemp(t)
	defer db.Close()

	mustMiss(t, db, "a")
	db.Put([]byte("a"), []byte("1"))
	mustGet(t, db, "a", "1")

	db.Put([]byte("a"), []byte("2"))
	mustGet(t, db, "a", "2")

	db.Put([]byte("empty"), nil)
	mustGet(t, db, "empty", "")

	if err := db.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	mustMiss(t, db, "a")
	if err := db.Delete([]byte("never-existed")); err != nil {
		t.Fatalf("deleting a missing key: %v", err)
	}
	if db.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", db.Len())
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	db, path := openTemp(t)
	db.Put([]byte("kept"), []byte("v1"))
	db.Put([]byte("kept"), []byte("v2"))
	db.Put([]byte("gone"), []byte("x"))
	db.Delete([]byte("gone"))

	db = reopen(t, db, path)
	mustGet(t, db, "kept", "v2")
	mustMiss(t, db, "gone")
	if got := db.Keys(); len(got) != 1 || got[0] != "kept" {
		t.Fatalf("Keys() = %v, want [kept]", got)
	}
}

// Simulates a crash halfway through writing a record.
func TestRecoversFromTornWrite(t *testing.T) {
	db, path := openTemp(t)
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))
	db.Close()

	partial := encode(record{key: []byte("c"), value: []byte("3")})
	appendBytes(t, path, partial[:len(partial)-1])

	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "a", "1")
	mustGet(t, db, "b", "2")
	mustMiss(t, db, "c")

	// New writes must land after the last good record, not after the garbage.
	db.Put([]byte("d"), []byte("4"))
	db = reopen(t, db, path)
	mustGet(t, db, "b", "2")
	mustGet(t, db, "d", "4")
}

func TestDropsRecordWithBadChecksum(t *testing.T) {
	db, path := openTemp(t)
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))
	db.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff // flip the last byte of b's value
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mustGet(t, db, "a", "1")
	mustMiss(t, db, "b")
}

func TestRejectsInvalidInput(t *testing.T) {
	db, _ := openTemp(t)
	defer db.Close()

	if err := db.Put(nil, []byte("v")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key: got %v, want ErrEmptyKey", err)
	}
	if err := db.Put(make([]byte, MaxKeySize+1), nil); err == nil {
		t.Fatal("oversized key: got nil error")
	}
}

func TestClosed(t *testing.T) {
	db, _ := openTemp(t)
	db.Close()

	if _, err := db.Get([]byte("a")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close: %v", err)
	}
	if err := db.Put([]byte("a"), nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Put after Close: %v", err)
	}
	if err := db.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("second Close: %v", err)
	}
}

// Run with -race to catch unsynchronized access.
func TestConcurrentAccess(t *testing.T) {
	db, path := openTemp(t)
	const workers, perWorker = 8, 200

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWorker {
				key := []byte(fmt.Sprintf("w%d-k%d", w, i))
				if err := db.Put(key, key); err != nil {
					t.Error(err)
					return
				}
				if _, err := db.Get(key); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	db = reopen(t, db, path)
	if db.Len() != workers*perWorker {
		t.Fatalf("Len() = %d, want %d", db.Len(), workers*perWorker)
	}
	mustGet(t, db, "w3-k150", "w3-k150")
}

func appendBytes(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkPut(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "data.kv"), Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	value := make([]byte, 100)
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		db.Put([]byte(fmt.Sprintf("key-%d", i)), value)
	}
}

func BenchmarkGet(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "data.kv"), Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	const n = 10000
	for i := range n {
		db.Put([]byte(fmt.Sprintf("key-%d", i)), make([]byte, 100))
	}
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		db.Get([]byte(fmt.Sprintf("key-%d", i%n)))
	}
}
