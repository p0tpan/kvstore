package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
)

// On-disk record layout (little-endian):
//
//	| crc (4) | keyLen (4) | valLen (4) | key | value |
//
// The CRC covers everything after itself. A valLen of tombstone marks a delete.
const (
	headerSize = 12
	tombstone  = math.MaxUint32

	MaxKeySize   = 64 << 10 // 64 KiB
	MaxValueSize = 64 << 20 // 64 MiB
)

// errCorrupt means a record is truncated or fails its checksum, which is what
// a crash in the middle of a write leaves behind.
var errCorrupt = errors.New("corrupt record")

type record struct {
	key     []byte
	value   []byte
	deleted bool
}

func encode(r record) []byte {
	valLen := uint32(len(r.value))
	if r.deleted {
		valLen = tombstone
	}
	buf := make([]byte, headerSize+len(r.key)+len(r.value))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(r.key)))
	binary.LittleEndian.PutUint32(buf[8:12], valLen)
	copy(buf[headerSize:], r.key)
	copy(buf[headerSize+len(r.key):], r.value)
	binary.LittleEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))
	return buf
}

// decode reads one record from rd and returns it with its encoded size.
// It returns io.EOF at a clean end of input and errCorrupt for a partial or
// damaged record.
func decode(rd io.Reader) (record, int64, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(rd, hdr[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return record{}, 0, errCorrupt
		}
		return record{}, 0, err
	}

	crc := binary.LittleEndian.Uint32(hdr[0:4])
	keyLen := binary.LittleEndian.Uint32(hdr[4:8])
	valLen := binary.LittleEndian.Uint32(hdr[8:12])
	deleted := valLen == tombstone
	if deleted {
		valLen = 0
	}
	// A garbage header could claim a huge length; don't try to allocate it.
	if keyLen > MaxKeySize || valLen > MaxValueSize {
		return record{}, 0, errCorrupt
	}

	body := make([]byte, keyLen+valLen)
	if _, err := io.ReadFull(rd, body); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return record{}, 0, errCorrupt
		}
		return record{}, 0, err
	}

	h := crc32.NewIEEE()
	h.Write(hdr[4:])
	h.Write(body)
	if h.Sum32() != crc {
		return record{}, 0, errCorrupt
	}

	r := record{key: body[:keyLen], value: body[keyLen:], deleted: deleted}
	return r, int64(headerSize + len(body)), nil
}
