package golsm

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/xiaoxuxiansheng/golsm/util"
)

// sst 文件中的数据块，和索引、过滤器为一一对应关系
type Block struct {
	conf       *Config       // lsm tree 配置文件
	buffer     [30]byte      // 用于辅助转移数据的临时缓冲区
	record     *bytes.Buffer // 用于复制溢写数据的缓冲区
	entriesCnt int           // kv 对数量
	prevKey    []byte        // 最晚一笔写入的数据的 key
}

// 数据块构造器
func NewBlock(conf *Config) *Block {
	return &Block{
		conf:   conf,
		record: bytes.NewBuffer([]byte{}),
	}
}

// 追加一组kv对到数据块中
func (b *Block) Append(key, value []byte) {
	// 兜底执行：设置 prevKey 为当前写入的 key；累加 entriesCnt 数量
	defer func() {
		b.prevKey = append(b.prevKey[:0], key...)
		b.entriesCnt++
	}()

	// 获取和之前 key 的共享key前缀长度
	sharedPrefixLen := util.SharedPrefixLen(b.prevKey, key)

	// 分别设置共享key长度||剩余key长度||值长度
	n := binary.PutUvarint(b.buffer[0:], uint64(sharedPrefixLen))
	n += binary.PutUvarint(b.buffer[n:], uint64(len(key)-sharedPrefixLen))
	n += binary.PutUvarint(b.buffer[n:], uint64(len(value)))

	// 将 共享key长度||剩余key长度||值长度 写入 record buffer
	_, _ = b.record.Write(b.buffer[:n])
	// 将 剩余key || value 写入 record buffer
	b.record.Write(key[sharedPrefixLen:])
	b.record.Write(value)
}

// 获取数据块的大小，单位 byte
func (b *Block) Size() int {
	return b.record.Len()
}

// 把块中的数据溢写到 dest writer 中
func (b *Block) FlushTo(dest io.Writer) (uint64, error) {
	defer b.clear()
	n, err := dest.Write(b.ToBytes())
	return uint64(n), err
}

// 将数据块中的数据转为 byte 数组
func (b *Block) ToBytes() []byte {
	return b.record.Bytes()
}

// 清理数据块中的数据
func (b *Block) clear() {
	b.entriesCnt = 0
	b.prevKey = b.prevKey[:0]
	b.record.Reset()
}
