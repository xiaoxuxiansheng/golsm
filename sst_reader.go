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
	if s.filterOffset == 0 || s.filterSize == 0 {
		if err := s.ReadFooter(); err != nil {
			return nil, err
		}
	}

	filterBlock, err := s.ReadBlock(s.filterOffset, s.filterSize)
	if err != nil {
		return nil, err
	}

	return s.readFilter(filterBlock)
}

// 读取索引块
func (s *SSTReader) ReadIndex() ([]*Index, error) {
	if s.indexOffset == 0 || s.indexSize == 0 {
		if err := s.ReadFooter(); err != nil {
			return nil, err
		}
	}

	indexBlock, err := s.ReadBlock(s.indexOffset, s.indexSize)
	if err != nil {
		return nil, err
	}

	return s.readIndex(indexBlock)
}

// 遍历数据
func (s *SSTReader) ReadData() ([]*KV, error) {
	if s.indexOffset == 0 || s.indexSize == 0 || s.filterOffset == 0 || s.filterSize == 0 {
		if err := s.ReadFooter(); err != nil {
			return nil, err
		}
	}

	dataBlock, err := s.ReadBlock(0, s.filterOffset)
	if err != nil {
		return nil, err
	}

	return s.ReadBlockData(dataBlock)
}

func (s *SSTReader) ReadBlock(offset, size uint64) ([]byte, error) {
	if _, err := s.src.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}
	s.reader.Reset(s.src)

	buf := make([]byte, size)
	_, err := io.ReadFull(s.reader, buf)
	return buf, err
}

func (s *SSTReader) readFilter(block []byte) (map[uint64][]byte, error) {
	blockToFilter := make(map[uint64][]byte)
	buf := bytes.NewBuffer(block)
	var prevKey []byte
	for {
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

func (s *SSTReader) readIndex(block []byte) ([]*Index, error) {
	var (
		index   []*Index
		prevKey []byte
	)

	buf := bytes.NewBuffer(block)
	for {
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

func (s *SSTReader) ReadBlockData(block []byte) ([]*KV, error) {
	var prevKey []byte
	buf := bytes.NewBuffer(block)
	var data []*KV

	for {
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
		prevKey = key
	}
	return data, nil
}

func (s *SSTReader) ReadRecord(prevKey []byte, buf *bytes.Buffer) (key, value []byte, err error) {
	sharedPrexLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, err
	}

	keyLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, err
	}

	valLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, err
	}

	key = make([]byte, keyLen)
	if _, err = io.ReadFull(buf, key); err != nil {
		return nil, nil, err
	}

	value = make([]byte, valLen)
	if _, err = io.ReadFull(buf, value); err != nil {
		return nil, nil, err
	}

	sharedPrefix := make([]byte, sharedPrexLen)
	copy(sharedPrefix, prevKey[:sharedPrexLen])
	key = append(sharedPrefix, key...)
	return
}
