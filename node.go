package golsm

import (
	"bytes"
	"os"
	"path"
)

type Node struct {
	conf          *Config           // 配置文件
	file          string            // sstable 对应的文件名，不含目录路径
	level         int               // sstable 所在 level 层级
	seq           int32             // sstable 的 seq 序列号. 对应为文件名中的 level_seq.sst 中的 seq
	size          uint64            // sstable 的大小，单位 byte
	blockToFilter map[uint64][]byte // 各 block 对应的 filter bitmap
	index         []*Index          // 各 block 对应的索引
	startKey      []byte            // sstable 中最小的 key
	endKey        []byte            // sstable 中最大的 key
	sstReader     *SSTReader        // 读取 sst 文件的 reader 入口
}

func NewNode(conf *Config, file string, sstReader *SSTReader, level int, seq int32, size uint64, blockToFilter map[uint64][]byte, index []*Index) *Node {
	return &Node{
		conf:          conf,
		file:          file,
		sstReader:     sstReader,
		level:         level,
		seq:           seq,
		size:          size,
		blockToFilter: blockToFilter,
		index:         index,
		startKey:      index[0].Key,
		endKey:        index[len(index)-1].Key,
	}
}

func (n *Node) GetAll() ([]*KV, error) {
	return n.sstReader.ReadData()
}

// 查看是否在节点中
func (n *Node) Get(key []byte) ([]byte, bool, error) {
	// 通过索引定位到具体的块
	index, ok := n.binarySearchIndex(key, 0, len(n.index)-1)
	if !ok {
		return nil, false, nil
	}

	// 布隆过滤器辅助判断 key 是否存在
	bitmap := n.blockToFilter[index.PrevBlockOffset]
	if ok = n.conf.Filter.Exist(bitmap, key); !ok {
		return nil, false, nil
	}

	// 读取对应的块
	block, err := n.sstReader.ReadBlock(index.PrevBlockOffset, index.PrevBlockSize)
	if err != nil {
		return nil, false, err
	}

	// 将块数据转为对应的 kv 对
	kvs, err := n.sstReader.ReadBlockData(block)
	if err != nil {
		return nil, false, err
	}

	for _, kv := range kvs {
		if bytes.Equal(kv.Key, key) {
			return kv.Value, true, nil
		}
	}

	return nil, false, nil
}

func (n *Node) Size() uint64 {
	return n.size
}

func (n *Node) Start() []byte {
	return n.startKey
}

func (n *Node) End() []byte {
	return n.endKey
}

func (n *Node) Index() (level int, seq int32) {
	level, seq = n.level, n.seq
	return
}

func (n *Node) Destory() {
	n.sstReader.Close()
	_ = os.Remove(path.Join(n.conf.Dir, n.file))
}

func (n *Node) Close() {
	n.sstReader.Close()
}

// 二分查找，key 可能从属的 block index
func (n *Node) binarySearchIndex(key []byte, start, end int) (*Index, bool) {
	if start == end {
		return n.index[start], bytes.Compare(n.index[start].Key, key) >= 0
	}

	// 目标块，保证 key <= index[i].key && key > index[i-1].key
	mid := start + (end-start)>>1
	if bytes.Compare(n.index[mid].Key, key) < 0 {
		return n.binarySearchIndex(key, mid+1, end)
	}

	return n.binarySearchIndex(key, start, mid)
}
