// Package storage implements a log-structured key-value store in the style of
// Bitcask: every write is appended to a single data file, and an in-memory
// index maps each live key to the position of its latest record.
package storage

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

var (
	ErrNotFound = errors.New("key not found")
	ErrClosed   = errors.New("database is closed")
	ErrEmptyKey = errors.New("key must not be empty")
)

type Options struct {
	// SyncWrites fsyncs after every write. Without it, a write that returned
	// successfully can still be lost if the machine (not just the process)
	// crashes before the OS flushes it to disk.
	SyncWrites bool
}

// entry locates a key's latest record in the data file.
type entry struct {
	offset int64
	size   int64
}

type DB struct {
	mu    sync.RWMutex
	file  *os.File
	index map[string]entry
	size  int64 // length of the valid log; the next record goes here
	opts  Options
}

// Open opens the data file at path, creating it if needed, and rebuilds the
// index by replaying the log.
func Open(path string, opts Options) (*DB, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	db := &DB{file: f, index: make(map[string]entry), opts: opts}
	if err := db.load(); err != nil {
		f.Close()
		return nil, fmt.Errorf("loading %s: %w", path, err)
	}
	return db, nil
}

// load replays the log from the start. If it hits a corrupt record, it
// truncates the file there: a torn write from a crash can only be at the tail,
// and nothing after an unreadable record can be trusted anyway.
func (db *DB) load() error {
	if _, err := db.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	rd := bufio.NewReader(db.file)

	var offset int64
	for {
		rec, n, err := decode(rd)
		if err == io.EOF {
			break
		}
		if errors.Is(err, errCorrupt) {
			if err := db.file.Truncate(offset); err != nil {
				return err
			}
			break
		}
		if err != nil {
			return err
		}

		if rec.deleted {
			delete(db.index, string(rec.key))
		} else {
			db.index[string(rec.key)] = entry{offset: offset, size: n}
		}
		offset += n
	}
	db.size = offset
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.file == nil {
		return nil, ErrClosed
	}

	e, ok := db.index[string(key)]
	if !ok {
		return nil, ErrNotFound
	}
	buf := make([]byte, e.size)
	if _, err := db.file.ReadAt(buf, e.offset); err != nil {
		return nil, err
	}
	rec, _, err := decode(bytes.NewReader(buf))
	if err != nil {
		return nil, fmt.Errorf("reading %q at offset %d: %w", key, e.offset, err)
	}
	return rec.value, nil
}

func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if len(key) > MaxKeySize {
		return fmt.Errorf("key is %d bytes, max is %d", len(key), MaxKeySize)
	}
	if len(value) > MaxValueSize {
		return fmt.Errorf("value is %d bytes, max is %d", len(value), MaxValueSize)
	}
	return db.append(record{key: key, value: value})
}

// Delete removes key. Deleting a missing key is not an error.
func (db *DB) Delete(key []byte) error {
	db.mu.RLock()
	_, ok := db.index[string(key)]
	db.mu.RUnlock()
	if !ok {
		return nil
	}
	return db.append(record{key: key, deleted: true})
}

func (db *DB) append(rec record) error {
	buf := encode(rec)

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.file == nil {
		return ErrClosed
	}

	if _, err := db.file.Write(buf); err != nil {
		// Cut off any partial record so later appends don't land after garbage.
		db.file.Truncate(db.size)
		return err
	}
	if db.opts.SyncWrites {
		if err := db.file.Sync(); err != nil {
			return err
		}
	}

	if rec.deleted {
		delete(db.index, string(rec.key))
	} else {
		db.index[string(rec.key)] = entry{offset: db.size, size: int64(len(buf))}
	}
	db.size += int64(len(buf))
	return nil
}

// Keys returns all live keys in sorted order.
func (db *DB) Keys() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	keys := make([]string, 0, len(db.index))
	for k := range db.index {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (db *DB) Len() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.index)
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.file == nil {
		return ErrClosed
	}
	syncErr := db.file.Sync()
	closeErr := db.file.Close()
	db.file = nil
	return errors.Join(syncErr, closeErr)
}
