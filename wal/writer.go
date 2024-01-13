package wal

import (
	"encoding/binary"
	"os"
)

// 预写日志写入口
type WALWriter struct {
	file         string   // 预写日志文件名，是包含了目录在内的绝对路径
	dest         *os.File // 预写日志文件
	assistBuffer [30]byte // 辅助转移数据使用的临时缓冲区
}

// 构造器
func NewWALWriter(file string) (*WALWriter, error) {
	// 打开 wal 文件，如果文件不存在则进行创建
	dest, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WALWriter{
		file: file,
		dest: dest,
	}, nil
}

// 写入一笔 kv 对到 wal 文件中
func (w *WALWriter) Write(key, value []byte) error {
	// 首先将key 和 value 长度填充到临时缓冲区 assistBuffer 中
	n := binary.PutUvarint(w.assistBuffer[0:], uint64(len(key)))
	n += binary.PutUvarint(w.assistBuffer[n:], uint64(len(value)))

	// 依次将 key 长度、val 长度、key、val 填充到 buf 中
	var buf []byte
	buf = append(buf, w.assistBuffer[:n]...)
	buf = append(buf, key...)
	buf = append(buf, value...)
	// 将以上内容写入到 wal 文件中
	_, err := w.dest.Write(buf)
	return err
}

func (w *WALWriter) Close() {
	_ = w.dest.Close()
}
