package filter

import (
	"testing"

	"github.com/spaolacci/murmur3"
)

func Test_BloomFilter_Add_Exist(t *testing.T) {
	m := 16
	bf, err := NewBloomFilter(m)
	if err != nil {
		t.Error(err)
		return
	}

	bf.Add([]byte("a"))
	bf.Add([]byte("b"))
	bf.Add([]byte("c"))
	bf.Add([]byte("d"))

	bitmap := bf.Hash()
	if ok := bf.Exist(bitmap, []byte("a")); !ok {
		t.Errorf("key: %v, expect: true, got: false", "a")
	}

	if ok := bf.Exist(bitmap, []byte("b")); !ok {
		t.Errorf("key: %v, expect: true, got: false", "b")
	}

	if ok := bf.Exist(bitmap, []byte("c")); !ok {
		t.Errorf("key: %v, expect: true, got: false", "c")
	}

	if ok := bf.Exist(bitmap, []byte("d")); !ok {
		t.Errorf("key: %v, expect: true, got: false", "d")
	}

	if ok := bf.Exist(bitmap, []byte("e")); ok {
		t.Errorf("key: %v, expect: false, got: true", "e")
	}
}

func Test_BloomFilter_Hash(t *testing.T) {
	m := 8
	bf, err := NewBloomFilter(m)
	if err != nil {
		t.Error(err)
		return
	}

	bf.Add([]byte("a"))
	bf.Add([]byte("b"))

	// k := 2
	// hashedKey1: 1009084850  delta: 33065692372498
	// hashedKey2: 2514386435  delta: 82391414721263
	// bitmap: 00011100
	expect := []byte{
		uint8(28),
		uint8(2),
	}

	if got := bf.Hash(); string(got) != string(expect) {
		t.Errorf("expect: %v, got: %v", expect, got)
	}
}

func Test_bitOperation(t *testing.T) {
	hashedKey1_1 := murmur3.Sum32([]byte("a"))
	t.Log(hashedKey1_1)
	t.Log(hashedKey1_1 & 7)
	hashedKey1_2 := hashedKey1_1 + (hashedKey1_1 >> 17) | (hashedKey1_1 << 15)
	t.Log(hashedKey1_2)
	t.Log(hashedKey1_2 & 7)
	hashedKey2_1 := murmur3.Sum32([]byte("b"))
	t.Log(hashedKey2_1)
	t.Log(hashedKey2_1 & 7)
	hashedKey2_2 := hashedKey2_1 + (hashedKey2_1 >> 17) | (hashedKey2_1 << 15)
	t.Log(hashedKey2_2)
	t.Log(hashedKey2_2 & 7)
}
