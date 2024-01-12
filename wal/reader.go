package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/xiaoxuxiansheng/golsm/memtable"
)

type WALReader struct {
	file   string
	src    *os.File
	reader *bufio.Reader
}

func NewWALReader(file string) (*WALReader, error) {
	src, err := os.OpenFile(file, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WALReader{
		file:   file,
		src:    src,
		reader: bufio.NewReader(src),
	}, nil
}

func (w *WALReader) RestoreToMemtable(memTable memtable.MemTable) error {
	body, err := io.ReadAll(w.reader)
	if err != nil {
		return err
	}

	defer func() {
		_, _ = w.src.Seek(0, io.SeekStart)
	}()

	kvs, err := w.readAll(bytes.NewReader(body))
	if err != nil {
		return err
	}
	for _, kv := range kvs {
		memTable.Put(kv.Key, kv.Value)
	}

	return nil
}

func (w *WALReader) readAll(reader *bytes.Reader) ([]*memtable.KV, error) {
	var kvs []*memtable.KV
	for {
		keyLen, err := binary.ReadUvarint(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		valLen, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, err
		}

		keyBuf := make([]byte, keyLen)
		if _, err = io.ReadFull(reader, keyBuf); err != nil {
			return nil, err
		}

		valBuf := make([]byte, valLen)
		if _, err = io.ReadFull(reader, valBuf); err != nil {
			return nil, err
		}

		kvs = append(kvs, &memtable.KV{
			Key:   keyBuf,
			Value: valBuf,
		})
	}

	return kvs, nil
}

func (w *WALReader) Close() {
	w.reader.Reset(w.src)
	_ = w.src.Close()
}
