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

// wal 文件读取器
type WALReader struct {
	file   string        // 预写日志文件名，是包含了目录在内的绝对路径
	src    *os.File      // 预写日志文件
	reader *bufio.Reader // 基于 bufio reader 对日志文件的封装
}

// 构造器函数.
func NewWALReader(file string) (*WALReader, error) {
	// 以只读模式打开 wal 文件，要求目标文件必须存在
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

// 读取 wal 文件，将所有内容注入到 memtable 中，以实现内存数据的复原
func (w *WALReader) RestoreToMemtable(memTable memtable.MemTable) error {
	// 读取 wal 文件全量内容
	body, err := io.ReadAll(w.reader)
	if err != nil {
		return err
	}

	// 兜底保证文件偏移量被重置到起始位置
	defer func() {
		_, _ = w.src.Seek(0, io.SeekStart)
	}()

	// 将文件中读取到的内容解析成一系列 kv 对
	kvs, err := w.readAll(bytes.NewReader(body))
	if err != nil {
		return err
	}

	// 将所有 kv 数据注入到 memtable 中
	for _, kv := range kvs {
		memTable.Put(kv.Key, kv.Value)
	}

	return nil
}

// 将文件中读到的原始内容解析成一系列 kv 对数据
func (w *WALReader) readAll(reader *bytes.Reader) ([]*memtable.KV, error) {
	var kvs []*memtable.KV
	// 循环读取每组 kv 对，直到遇到 eof 错误才终止流程
	for {
		// 从 reader 中读取首个 uint64 作为 key 长度
		keyLen, err := binary.ReadUvarint(reader)
		// 如果遇到 eof 错误说明文件内容已经读取完毕，终止流程
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		// 从 reader 中读取下一个 uint64 作为 val 长度
		valLen, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, err
		}

		// 从 reader 中读取对应于 key 长度的字节数作为 key
		keyBuf := make([]byte, keyLen)
		if _, err = io.ReadFull(reader, keyBuf); err != nil {
			return nil, err
		}

		// 从 reader 中读取对应于 val 长度的字节数作为 val
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
