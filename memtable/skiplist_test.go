package memtable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Skiplist(t *testing.T) {
	skiplist := NewSkiplist()
	skiplist.Put([]byte("a"), []byte("b"))
	skiplist.Put([]byte("a"), []byte("c"))
	skiplist.Put([]byte("ab"), []byte("aa"))
	skiplist.Put([]byte("abc"), []byte("aaa"))
	skiplist.Put([]byte("bc"), []byte("bbb"))
	skiplist.Put([]byte("ab"), []byte("bb"))

	val, _ := skiplist.Get([]byte("a"))
	assert.Equal(t, val, []byte("c"))
	val, _ = skiplist.Get([]byte("ab"))
	assert.Equal(t, val, []byte("bb"))
	val, _ = skiplist.Get([]byte("abc"))
	assert.Equal(t, val, []byte("aaa"))
	val, _ = skiplist.Get([]byte("bc"))
	assert.Equal(t, val, []byte("bbb"))
	_, ok := skiplist.Get([]byte("bcd"))
	assert.Equal(t, ok, false)
	assert.Equal(t, skiplist.Size(), 4)

	kvs := skiplist.All()
	assert.Equal(t, len(kvs), 4)

	assert.Equal(t, kvs[0].Key, []byte("a"))
	assert.Equal(t, kvs[0].Value, []byte("c"))

	assert.Equal(t, kvs[1].Key, []byte("ab"))
	assert.Equal(t, kvs[1].Value, []byte("bb"))

	assert.Equal(t, kvs[2].Key, []byte("abc"))
	assert.Equal(t, kvs[2].Value, []byte("aaa"))

	assert.Equal(t, kvs[3].Key, []byte("bc"))
	assert.Equal(t, kvs[3].Value, []byte("bbb"))
}
