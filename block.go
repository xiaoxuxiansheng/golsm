package golsm

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/xiaoxuxiansheng/golsm/util"
)

type Block struct {
	conf       *Config       // lsm tree 配置问津
	buffer     [30]byte      // 临时缓冲区
	record     *bytes.Buffer // 记录缓冲区
	entriesCnt int           // kv 对数量
	prevKey    []byte        // 最晚一笔写入的数据的 key
}

func NewBlock(conf *Config) *Block {
	return &Block{
		conf:   conf,
		record: bytes.NewBuffer([]byte{}),
	}
}

// 追加一组kv对到数据块中
func (b *Block) Append(key, value []byte) {
	defer func() {
		b.prevKey = append(b.prevKey[:0], key...)
		b.entriesCnt++
	}()

	// 共享前缀长度
	sharedPrefixLen := util.SharedPrefixLen(b.prevKey, key)

	// 共享键长度、剩余键长度、值长度、剩余键、剩余值
	n := binary.PutUvarint(b.buffer[0:], uint64(sharedPrefixLen))
	n += binary.PutUvarint(b.buffer[n:], uint64(len(key)-sharedPrefixLen))
	n += binary.PutUvarint(b.buffer[n:], uint64(len(value)))

	_, _ = b.record.Write(b.buffer[:n])
	b.record.Write(key[sharedPrefixLen:])
	b.record.Write(value)
}

func (b *Block) Size() int {
	return b.record.Len()
}

// 把块中的数据溢写到 writer 中
func (b *Block) FlushTo(dest io.Writer) (uint64, error) {
	defer b.clear()
	n, err := dest.Write(b.ToBytes())
	return uint64(n), err
}

func (b *Block) ToBytes() []byte {
	return b.record.Bytes()
}

// 清理快中的数据
func (b *Block) clear() {
	b.entriesCnt = 0
	b.prevKey = b.prevKey[:0]
	b.record.Reset()
}
