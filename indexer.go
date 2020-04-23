// Package mph is a Go implementation of the compress, hash and displace (CHD)
// minimal perfect hash algorithm.
//
// See http://cmph.sourceforge.net/papers/esa09.pdf for details.
//
// To create and serialize a hash table:
//
//		b := mph.Builder()
// 		for k, v := range data {
// 			b.Add(k, v)
// 		}
// 		h, _ := b.Build()
// 		w, _ := os.Create("data.idx")
// 		b, _ := h.Write(w)
//
// To read from the hash table:
//
//		r, _ := os.Open("data.idx")
//		h, _ := h.Read(r)
//
//		v := h.Get([]byte("some key"))
//		if v == nil {
//		    // Key not found
//		}
//
// MMAP is also indirectly supported, by deserializing from a byte
// slice and slicing the keys and values.
//
// See https://github.com/alecthomas/mph for source.
package mph

import (
	"encoding/binary"
	"io"
	"io/ioutil"
)

// CHD hash table lookup.
type Indexer struct {
	// Random hash function table.
	r []uint64
	// Array of indices into hash function table r. We assume there aren't
	// more than 2^16 hash functions O_o
	indices []uint16
}

func hasher(data []byte) uint64 {
	var hash uint64 = 14695981039346656037
	for _, c := range data {
		hash ^= uint64(c)
		hash *= 1099511628211
	}
	return hash
}

// Read a serialized CHD.
func ReadIndexer(r io.Reader) (*Indexer, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return MmapIndexer(b)
}

// Mmap creates a new CHD aliasing the CHD structure over an existing byte region (typically mmapped).
func mmapIndexer(b []byte, bi *sliceReader) (*Indexer, error) {
	c := &Indexer{}

	// Read vector of hash functions.
	rl := bi.ReadInt()
	c.r = bi.ReadUint64Array(rl)

	// Read hash function indices.
	il := bi.ReadInt()
	c.indices = bi.ReadUint16Array(il)

	return c, nil
}

// Mmap creates a new CHD aliasing the CHD structure over an existing byte region (typically mmapped).
func MmapIndexer(b []byte) (*Indexer, error) {
	bi := &sliceReader{b: b}
	return mmapIndexer(b, bi)
}

// Get an entry from the hash table.
func (c *Indexer) Get(key []byte, numKeys int) uint64 {
	r0 := c.r[0]
	h := hasher(key) ^ r0
	i := h % uint64(len(c.indices))
	ri := c.indices[i]
	// This can occur if there were unassigned slots in the hash table.
	if ri >= uint16(len(c.r)) {
		return 0
	}
	r := c.r[ri]
	return (h ^ r) % uint64(numKeys)
}

func write(w io.Writer, nd ...interface{}) error {
	for _, d := range nd {
		if err := binary.Write(w, binary.LittleEndian, d); err != nil {
			return err
		}
	}
	return nil
}

// Serialize the CHD. The serialized form is conducive to mmapped access. See
// the Mmap function for details.
func (c *Indexer) Write(w io.Writer) error {
	data := []interface{}{
		uint32(len(c.r)), c.r,
		uint32(len(c.indices)), c.indices,
	}

	if err := write(w, data...); err != nil {
		return err
	}
	return nil
}
