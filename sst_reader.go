package golsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path"
)

// kv 对
type KV struct {
	Key   []byte
	Value []byte
}

// 对应于 lsm tree 中的一个 sstable. 这是读取流程的视角
type SSTReader struct {
	conf         *Config       // 配置文件
	src          *os.File      // 对应的文件
	reader       *bufio.Reader // 读取文件的 reader
	filterOffset uint64        // 过滤器块起始位置在 sstable 的 offset
	filterSize   uint64        // 过滤器块的大小，单位 byte
	indexOffset  uint64        // 索引块起始位置在 sstable 的 offset
	indexSize    uint64        // 索引块的大小，单位 byte
}

// sstReader 构造器
func NewSSTReader(file string, conf *Config) (*SSTReader, error) {
	src, err := os.OpenFile(path.Join(conf.Dir, file), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &SSTReader{
		conf:   conf,
		src:    src,
		reader: bufio.NewReader(src),
	}, nil
}

// sstable 数据大小，单位 byte
func (s *SSTReader) Size() (uint64, error) {
	if s.indexOffset == 0 {
		if err := s.ReadFooter(); err != nil {
			return 0, err
		}
	}
	return s.indexOffset + s.indexSize, nil
}

func (s *SSTReader) Close() {
	s.reader.Reset(s.src)
	_ = s.src.Close()
}

// 读取 sstable footer 信息，赋给 sstreader 的成员属性
func (s *SSTReader) ReadFooter() error {
	// 从尾部开始倒退 sst footer size 大小的偏移量
	if _, err := s.src.Seek(-int64(s.conf.SSTFooterSize), io.SeekEnd); err != nil {
		return err
	}

	s.reader.Reset(s.src)

	var err error
	if s.filterOffset, err = binary.ReadUvarint(s.reader); err != nil {
		return err
	}

	if s.filterSize, err = binary.ReadUvarint(s.reader); err != nil {
		return err
	}

	if s.indexOffset, err = binary.ReadUvarint(s.reader); err != nil {
		return err
	}

	if s.indexSize, err = binary.ReadUvarint(s.reader); err != nil {
		return err
	}

	return nil
}

// 读取过滤器
func (s *SSTReader) ReadFilter() (map[uint64][]byte, error) {
	// 如果 footer 信息还没读取，则先完成 footer 信息加载
	if s.filterOffset == 0 || s.filterSize == 0 {
		if err := s.ReadFooter(); err != nil {
			return nil, err
		}
	}

	// 读取 filter block 块的内容
	filterBlock, err := s.ReadBlock(s.filterOffset, s.filterSize)
	if err != nil {
		return nil, err
	}

	// 对 filter block 块的内容进行解析
	return s.readFilter(filterBlock)
}

// 读取索引块
func (s *SSTReader) ReadIndex() ([]*Index, error) {
	// 如果 footer 信息还没读取，则先完成 footer 信息加载
	if s.indexOffset == 0 || s.indexSize == 0 {
		if err := s.ReadFooter(); err != nil {
			return nil, err
		}
	}

	// 读取 index block 块的内容
	indexBlock, err := s.ReadBlock(s.indexOffset, s.indexSize)
	if err != nil {
		return nil, err
	}

	// 对 index block 块的内容进行解析
	return s.readIndex(indexBlock)
}

// 读取 sstable 下的全量 kv 数据
func (s *SSTReader) ReadData() ([]*KV, error) {
	// 如果 footer 信息还没读取，则先完成 footer 信息加载
	if s.indexOffset == 0 || s.indexSize == 0 || s.filterOffset == 0 || s.filterSize == 0 {
		if err := s.ReadFooter(); err != nil {
			return nil, err
		}
	}

	// 读取所有 data block 的内容
	dataBlock, err := s.ReadBlock(0, s.filterOffset)
	if err != nil {
		return nil, err
	}

	// 解析所有 data block 的内容
	return s.ReadBlockData(dataBlock)
}

// 读取一个 block 块的内容
func (s *SSTReader) ReadBlock(offset, size uint64) ([]byte, error) {
	// 根据起始偏移量，设置文件的 offset
	if _, err := s.src.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}
	s.reader.Reset(s.src)

	// 读取指定 size 的内容
	buf := make([]byte, size)
	_, err := io.ReadFull(s.reader, buf)
	return buf, err
}

// 解析 filter block 块的内容
func (s *SSTReader) readFilter(block []byte) (map[uint64][]byte, error) {
	blockToFilter := make(map[uint64][]byte)
	// 将 filter block 块内容封装成一个 buffer
	buf := bytes.NewBuffer(block)
	var prevKey []byte
	for {
		// 每次读取一条 block filter 记录，key 为 block 的 offset，value 为过滤器 bitmap
		key, value, err := s.ReadRecord(prevKey, buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		blockOffset, _ := binary.Uvarint(key)
		blockToFilter[blockOffset] = value
		prevKey = key
	}

	return blockToFilter, nil
}

// 解析 index block 块的内容
func (s *SSTReader) readIndex(block []byte) ([]*Index, error) {
	var (
		index   []*Index
		prevKey []byte
	)

	// 将 index block 块内容封装成一个 buffer
	buf := bytes.NewBuffer(block)
	for {
		// 每次读取一条 index 记录，key 为 block 之间的分隔键，value 为前一个 block 的 offset 和 size
		key, value, err := s.ReadRecord(prevKey, buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		blockOffset, n := binary.Uvarint(value)
		blockSize, _ := binary.Uvarint(value[n:])
		index = append(index, &Index{
			Key:             key,
			PrevBlockOffset: blockOffset,
			PrevBlockSize:   blockSize,
		})

		prevKey = key
	}
	return index, nil
}

// 读取某个 block 的数据
func (s *SSTReader) ReadBlockData(block []byte) ([]*KV, error) {
	// 需要临时记录前一个 key 的内容
	var prevKey []byte
	// block 数据封装成 buffer
	buf := bytes.NewBuffer(block)
	var data []*KV

	for {
		// 每次读取一条 kv 对
		key, value, err := s.ReadRecord(prevKey, buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		data = append(data, &KV{
			Key:   key,
			Value: value,
		})
		// 对 prevKey 进行更新
		prevKey = key
	}
	return data, nil
}

// 读取一条 kv 对数据
func (s *SSTReader) ReadRecord(prevKey []byte, buf *bytes.Buffer) (key, value []byte, err error) {
	// 获取当前 key 和 prevKey 的共享前缀长度
	sharedPrexLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, err
	}

	// 获取当前 key 剩余部分长度
	keyLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, err
	}

	// 获取 val 长度
	valLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, err
	}

	// 读取 key 剩余部分
	key = make([]byte, keyLen)
	if _, err = io.ReadFull(buf, key); err != nil {
		return nil, nil, err
	}

	// 读取 val
	value = make([]byte, valLen)
	if _, err = io.ReadFull(buf, value); err != nil {
		return nil, nil, err
	}

	// 拼接 key 共享前缀 + 剩余部分
	sharedPrefix := make([]byte, sharedPrexLen)
	copy(sharedPrefix, prevKey[:sharedPrexLen])
	key = append(sharedPrefix, key...)
	// 返回完整的 key、val
	return
}
