package mph

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	sampleData = map[string]string{
		"one":   "1",
		"two":   "2",
		"three": "3",
		"four":  "4",
		"five":  "5",
		"six":   "6",
		"seven": "7",
	}
)

var (
	words [][]byte
)

func init() {
	f, err := os.Open("/usr/share/dict/words")
	if err != nil {
		panic(err)
	}
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		words = append(words, line)
	}
}

func TestCHDBuilder(t *testing.T) {
	b := Builder()
	for k, v := range sampleData {
		b.Add([]byte(k), []byte(v))
	}
	c, err := b.Build()
	assert.NoError(t, err)
	assert.Equal(t, 7, len(c.keys))
	for k, v := range sampleData {
		assert.Equal(t, []byte(v), c.Get([]byte(k)))
	}
	assert.Nil(t, c.Get([]byte("monkey")))
}

func TestCHDSerialization(t *testing.T) {
	cb := Builder()
	for _, v := range words {
		cb.Add([]byte(v), []byte(v))
	}
	m, err := cb.Build()
	assert.NoError(t, err)
	w := &bytes.Buffer{}
	err = m.Write(w)
	assert.NoError(t, err)

	indexW := &bytes.Buffer{}
	z := gzip.NewWriter(indexW)
	err = m.Indexer.Write(z)
	assert.NoError(t, err)
	z.Close()
	fmt.Printf("size: indexer %d vs. hashmap %d, num keys: %d, hash fns %d. indexes %d, Eg, %x & %x\n", len(indexW.Bytes()), len(w.Bytes()), len(m.keys), len(m.Indexer.r), len(m.Indexer.indices), m.Indexer.indices[0], m.Indexer.indices[100])	
	
	n, err := Mmap(w.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, n.Indexer.r, m.Indexer.r)
	assert.Equal(t, n.Indexer.indices, m.Indexer.indices)
	assert.Equal(t, n.keys, m.keys)
	assert.Equal(t, n.values, m.values)
	for _, v := range words {
		assert.Equal(t, []byte(v), n.Get([]byte(v)))
	}
}
func TestIndexerSerialization(t *testing.T) {
	cb := Builder()
	for _, v := range words {
		cb.Add([]byte(v), []byte(v))
	}
	m, err := cb.Build()
	assert.NoError(t, err)
	w := &bytes.Buffer{}
	err = m.Write(w)
	assert.NoError(t, err)

	buf := &bytes.Buffer{}
	z := gzip.NewWriter(buf)
	err = m.Indexer.Write(z)
	assert.NoError(t, err)
	z.Close()
	fmt.Printf("size: indexer %d,  num keys: %d\n", len(buf.Bytes()), len(m.keys))	

	rZ, err := gzip.NewReader(buf)
	assert.NoError(t, err)

	indexer, err := ReadIndexer(rZ)
	assert.NoError(t, err)

	n, err := Mmap(w.Bytes())
	assert.NoError(t, err)
	n.Indexer = *indexer
	assert.Equal(t, n.Indexer.r, m.Indexer.r)
	assert.Equal(t, n.Indexer.indices, m.Indexer.indices)
	assert.Equal(t, n.keys, m.keys)
	assert.Equal(t, n.values, m.values)
	for _, v := range words {
		assert.Equal(t, []byte(v), n.Get([]byte(v)))
	}
}

func TestCHDSerialization_empty(t *testing.T) {
	cb := Builder()
	m, err := cb.Build()
	assert.NoError(t, err)
	w := &bytes.Buffer{}
	err = m.Write(w)
	assert.NoError(t, err)

	n, err := Mmap(w.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, n.Indexer.r, m.Indexer.r)
	assert.Equal(t, n.Indexer.indices, m.Indexer.indices)
	assert.Equal(t, n.keys, m.keys)
	assert.Equal(t, n.values, m.values)
}

func TestCHDSerialization_one(t *testing.T) {
	cb := Builder()
	cb.Add([]byte("k"), []byte("v"))
	m, err := cb.Build()
	assert.NoError(t, err)
	w := &bytes.Buffer{}
	err = m.Write(w)
	assert.NoError(t, err)

	n, err := Mmap(w.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, n.Indexer.r, m.Indexer.r)
	assert.Equal(t, n.Indexer.indices, m.Indexer.indices)
	assert.Equal(t, n.keys, m.keys)
	assert.Equal(t, n.values, m.values)
}

func BenchmarkBuiltinMap(b *testing.B) {
	keys := []string{}
	d := map[string]string{}
	for _, bk := range words {
		k := string(bk)
		d[k] = k
		keys = append(keys, k)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d[keys[i%len(keys)]]
	}
}

func BenchmarkCHD(b *testing.B) {
	keys := [][]byte{}
	mph := Builder()
	for _, k := range words {
		keys = append(keys, k)
		mph.Add(k, k)
	}
	h, _ := mph.Build()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Get(keys[i%len(keys)])
	}
}
