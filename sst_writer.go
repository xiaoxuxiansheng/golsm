package golsm

import (
	"bytes"
	"encoding/binary"
	"os"
	"path"

	"github.com/xiaoxuxiansheng/golsm/util"
)

// sstable 中用于快速检索 block 的索引
type Index struct {
	Key             []byte // 索引的 key. 保证其 >= 前一个 block 最大 key； < 后一个 block 的最小 key
	PrevBlockOffset uint64 // 索引前一个 block 起始位置在 sstable 中对应的 offset
	PrevBlockSize   uint64 // 索引前一个 block 的大小，单位 byte
}

// 对应于 lsm tree 中的一个 sstable. 这是写入流程的视角
type SSTWriter struct {
	conf          *Config           // 配置文件
	dest          *os.File          // sstable 对应的磁盘文件
	dataBuf       *bytes.Buffer     // 数据块缓冲区 key -> val
	filterBuf     *bytes.Buffer     // 过滤器块缓冲区 prev block offset -> filter bit map
	indexBuf      *bytes.Buffer     // 索引块缓冲区 index key -> prev block offset, prev block size
	blockToFilter map[uint64][]byte // prev block offset -> filter bit map
	index         []*Index          // index key -> prev block offset, prev block size

	dataBlock     *Block   // 数据块
	filterBlock   *Block   // 过滤器块
	indexBlock    *Block   // 索引块
	assistScratch [20]byte // 用于在写索引块时临时使用的辅助缓冲区

	prevKey         []byte // 前一笔数据的 key
	prevBlockOffset uint64 // 前一个数据块的起始偏移位置
	prevBlockSize   uint64 // 前一个数据块的大小
}

func NewSSTWriter(file string, conf *Config) (*SSTWriter, error) {
	dest, err := os.OpenFile(path.Join(conf.Dir, file), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &SSTWriter{
		conf:          conf,
		dest:          dest,
		dataBuf:       bytes.NewBuffer([]byte{}),
		filterBuf:     bytes.NewBuffer([]byte{}),
		indexBuf:      bytes.NewBuffer([]byte{}),
		blockToFilter: make(map[uint64][]byte),
		dataBlock:     NewBlock(conf),
		filterBlock:   NewBlock(conf),
		indexBlock:    NewBlock(conf),
		prevKey:       []byte{},
	}, nil
}

// 完成 sstable 的全部处理流程，包括将其中的数据溢写到磁盘，并返回信息供上层的 lsm 获取缓存
func (s *SSTWriter) Finish() (size uint64, blockToFilter map[uint64][]byte, index []*Index) {
	// 完成最后一个块的处理
	s.refreshBlock()
	// 补齐最后一个 index
	s.insertIndex(s.prevKey)

	// 将布隆过滤器块写入缓冲区
	_, _ = s.filterBlock.FlushTo(s.filterBuf)
	// 将索引块写入缓冲区
	_, _ = s.indexBlock.FlushTo(s.indexBuf)

	// 处理 footer，记录布隆过滤器块起始、大小、索引块起始、大小
	footer := make([]byte, s.conf.SSTFooterSize)
	size = uint64(s.dataBuf.Len())
	n := binary.PutUvarint(footer[0:], size)
	filterBufLen := uint64(s.filterBuf.Len())
	n += binary.PutUvarint(footer[n:], filterBufLen)
	size += filterBufLen
	n += binary.PutUvarint(footer[n:], size)
	indexBufLen := uint64(s.indexBuf.Len())
	n += binary.PutUvarint(footer[n:], indexBufLen)
	size += indexBufLen

	// 依次写入文件
	_, _ = s.dest.Write(s.dataBuf.Bytes())
	_, _ = s.dest.Write(s.filterBuf.Bytes())
	_, _ = s.dest.Write(s.indexBuf.Bytes())
	_, _ = s.dest.Write(footer)

	blockToFilter = s.blockToFilter
	index = s.index
	return
}

// 追加一笔数据到 sstable 中
func (s *SSTWriter) Append(key, value []byte) {
	// 倘若开启一个新的数据块，需要添加索引
	if s.dataBlock.entriesCnt == 0 {
		s.insertIndex(key)
	}

	// 将数据写入到数据块中
	s.dataBlock.Append(key, value)
	// 将 key 添加到块的布隆过滤器中
	s.conf.Filter.Add(key)
	// 记录一下最新的 key
	s.prevKey = key

	// 倘若数据块大小超限，则需要将其添加到 dataBuffer，并重置块
	if s.dataBlock.Size() >= s.conf.SSTDataBlockSize {
		s.refreshBlock()
	}
}

func (s *SSTWriter) Size() uint64 {
	return uint64(s.dataBuf.Len())
}

func (s *SSTWriter) Close() {
	_ = s.dest.Close()
	s.dataBuf.Reset()
	s.indexBuf.Reset()
	s.filterBuf.Reset()
}

func (s *SSTWriter) insertIndex(key []byte) {
	// 获取索引的 key
	indexKey := util.GetSeparatorBetween(s.prevKey, key)
	n := binary.PutUvarint(s.assistScratch[0:], s.prevBlockOffset)
	n += binary.PutUvarint(s.assistScratch[n:], s.prevBlockSize)

	s.indexBlock.Append(indexKey, s.assistScratch[:n])
	s.index = append(s.index, &Index{
		Key:             indexKey,
		PrevBlockOffset: s.prevBlockOffset,
		PrevBlockSize:   s.prevBlockSize,
	})
}

func (s *SSTWriter) refreshBlock() {
	if s.conf.Filter.KeyLen() == 0 {
		return
	}

	s.prevBlockOffset = uint64(s.dataBuf.Len())
	// 添加布隆过滤器 bitmap
	filterBitmap := s.conf.Filter.Hash()
	s.blockToFilter[s.prevBlockOffset] = filterBitmap
	n := binary.PutUvarint(s.assistScratch[0:], s.prevBlockOffset)
	s.filterBlock.Append(s.assistScratch[:n], filterBitmap)
	// 重置布隆过滤器
	s.conf.Filter.Reset()

	// 将 block 的数据添加到缓冲区
	s.prevBlockSize, _ = s.dataBlock.FlushTo(s.dataBuf)
}
