package golsm

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func Test_Block_ToBytes(t *testing.T) {
	block := NewBlock(NewConfig("./"))
	// 0 | 1 | 1 | a | b
	block.Append([]byte("a"), []byte("b"))
	// 0 | 1 | 1 | a | b | 0 | 1 | 1 | b | c
	block.Append([]byte("b"), []byte("c"))
	// 0 | 1 | 1 | a | b | 0 | 1 | 1 | b | c | 1 | 2 | 1 | cd | d
	block.Append([]byte("bcd"), []byte("d"))
	// 0 | 1 | 1 | a | b | 0 | 1 | 1 | b | c | 1 | 2 | 1 | cd | d | 2 | 1 | 1 | e | e
	block.Append([]byte("bce"), []byte("e"))

	expect := bytes.NewBuffer([]byte{})
	// record: 0 | 1 | 1 | a | b | 0 | 1 | 1 | b | c | 1 | 2 | 1 | cd | d | 2 | 1 | 1 | e | e
	var recordBuf [8]byte
	n := binary.PutUvarint(recordBuf[0:], uint64(0))
	expect.Write(recordBuf[:n])
	n = binary.PutUvarint(recordBuf[0:], uint64(1))
	expect.Write(recordBuf[:n])
	n = binary.PutUvarint(recordBuf[0:], uint64(1))
	expect.Write(recordBuf[:n])
	expect.Write([]byte{'a', 'b'})
	n = binary.PutUvarint(recordBuf[0:], uint64(0))
	expect.Write(recordBuf[:n])
	n = binary.PutUvarint(recordBuf[0:], uint64(1))
	expect.Write(recordBuf[:n])
	n = binary.PutUvarint(recordBuf[0:], uint64(1))
	expect.Write(recordBuf[:n])
	expect.Write([]byte{'b', 'c'})
	n = binary.PutUvarint(recordBuf[0:], uint64(1))
	expect.Write(recordBuf[:n])
	n = binary.PutUvarint(recordBuf[0:], uint64(2))
	expect.Write(recordBuf[:n])
	n = binary.PutUvarint(recordBuf[0:], uint64(1))
	expect.Write(recordBuf[:n])
	expect.Write([]byte{'c', 'd', 'd'})
	n = binary.PutUvarint(recordBuf[0:], uint64(2))
	expect.Write(recordBuf[:n])
	n = binary.PutUvarint(recordBuf[0:], uint64(1))
	expect.Write(recordBuf[:n])
	n = binary.PutUvarint(recordBuf[0:], uint64(1))
	expect.Write(recordBuf[:n])
	expect.Write([]byte{'e', 'e'})

	if got := block.ToBytes(); string(got) != expect.String() {
		t.Errorf("expect: %v, got: %v", expect, got)
	}
}
