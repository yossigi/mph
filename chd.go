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
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
)

// CHD hash table lookup.
type CHD struct {
	Indexer Indexer
	// Final table of values.
	keys   [][]byte
	values [][]byte
}

// Read a serialized CHD.
func Read(r io.Reader) (*CHD, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return Mmap(b)
}

// Mmap creates a new CHD aliasing the CHD structure over an existing byte region (typically mmapped).
func Mmap(b []byte) (*CHD, error) {
	bi := &sliceReader{b: b}
	idx, err := mmapIndexer(b, bi)
	if err != nil {
		return nil, err
	}
	c := &CHD{Indexer: *idx}

	el := bi.ReadInt()

	c.keys = make([][]byte, el)
	c.values = make([][]byte, el)

	for i := uint64(0); i < el; i++ {
		kl := bi.ReadInt()
		vl := bi.ReadInt()
		c.keys[i] = bi.Read(kl)
		c.values[i] = bi.Read(vl)
	}

	return c, nil
}

// Get an entry from the hash table.
func (c *CHD) Get(key []byte) []byte {
	ti := c.Indexer.Get(key, len(c.keys))
	// fmt.Printf("r[0]=%d, h=%d, i=%d, ri=%d, r=%d, ti=%d\n", c.r[0], h, i, ri, r, ti)
	k := c.keys[ti]
	if bytes.Compare(k, key) != 0 {
		return nil
	}
	v := c.values[ti]
	return v
}

func (c *CHD) Len() int {
	return len(c.keys)
}

// Iterate over entries in the hash table.
func (c *CHD) Iterate() *Iterator {
	if len(c.keys) == 0 {
		return nil
	}
	return &Iterator{c: c}
}

// Serialize the CHD. The serialized form is conducive to mmapped access. See
// the Mmap function for details.
func (c *CHD) Write(w io.Writer) error {
	write := func(nd ...interface{}) error {
		for _, d := range nd {
			if err := binary.Write(w, binary.LittleEndian, d); err != nil {
				return err
			}
		}
		return nil
	}

/*
	err := c.Indexer.Write(w)
	if err != nil {
		return err
	}
*/
	data := []interface{}{
		uint32(len(c.Indexer.r)), c.Indexer.r,
		uint32(len(c.Indexer.indices)), c.Indexer.indices,
		uint32(len(c.keys)),
	}

	if err := write(data...); err != nil {
		return err
	}

	for i := range c.keys {
		k, v := c.keys[i], c.values[i]
		if err := write(uint32(len(k)), uint32(len(v))); err != nil {
			return err
		}
		if _, err := w.Write(k); err != nil {
			return err
		}
		if _, err := w.Write(v); err != nil {
			return err
		}
	}
	return nil
}

type Iterator struct {
	i int
	c *CHD
}

func (c *Iterator) Get() (key []byte, value []byte) {
	return c.c.keys[c.i], c.c.values[c.i]
}

func (c *Iterator) Next() *Iterator {
	c.i++
	if c.i >= len(c.c.keys) {
		return nil
	}
	return c
}
