package memtable

import (
	"bytes"
	"math/rand"
	"time"
)

// 跳表，未加锁，不保证并发安全
type Skiplist struct {
	head      *skipNode // 跳表的头结点
	entrisCnt int       // 跳表中的 kv 对个数
	size      int       // 跳表数据量大小，单位 byte
}

// 跳表节点
type skipNode struct {
	nexts      []*skipNode // 通过 next slice 来实现跳表节点多层指针结构
	key, value []byte      // 节点内存储的 kv 对数据
}

// 构造跳表实例
func NewSkiplist() MemTable {
	return &Skiplist{
		head: &skipNode{}, // 需要初始化根节点
	}
}

// 写入一笔 kv 对到跳表. 如果 key 不存在，则为插入操作；如果 key 已存在则为覆盖操作
func (s *Skiplist) Put(key, value []byte) {
	// 倘若 key 已存在
	if node := s.getNode(key); node != nil {
		// 根据新老 value dif 值，调整 skiplist 数据量 size 大小
		s.size += (len(value) - len(node.value))
		// 覆盖之
		node.value = value
		return
	}

	// key 不存在，则为插入行为. 在跳表 size 基础上加上 key 和 value 的大小
	s.size += (len(key) + len(value))
	s.entrisCnt++
	// roll 出新节点高度
	newNodeHeight := s.roll()

	// 倘若跳表原高度不足，则补齐高度
	if len(s.head.nexts) < newNodeHeight {
		dif := make([]*skipNode, newNodeHeight+1-len(s.head.nexts))
		s.head.nexts = append(s.head.nexts, dif...)
	}

	// 构造新节点
	newNode := skipNode{
		nexts: make([]*skipNode, newNodeHeight),
		key:   key,
		value: value,
	}

	// 层数自高向低，每层按序插入节点
	move := s.head
	for level := newNodeHeight - 1; level >= 0; level-- {
		// 层内持续向右遍历，直到右侧节点不存在或者 key 值更大
		for move.nexts[level] != nil && bytes.Compare(move.nexts[level].key, key) < 0 {
			move = move.nexts[level]
		}

		// 插入节点
		newNode.nexts[level] = move.nexts[level]
		move.nexts[level] = &newNode
	}
}

// 从跳表中读取 kv 对
func (s *Skiplist) Get(key []byte) ([]byte, bool) {
	// 倘若 key 存在，返回对应 val
	if node := s.getNode(key); node != nil {
		return node.value, true
	}

	return nil, false
}

// 获取跳表中全量 kv 对数据
func (s *Skiplist) All() []*KV {
	if len(s.head.nexts) == 0 {
		return nil
	}

	kvs := make([]*KV, 0, s.entrisCnt)
	// 从第 0 层开始自左向右依次遍历读取
	for move := s.head; move.nexts[0] != nil; move = move.nexts[0] {
		kvs = append(kvs, &KV{
			Key:   move.nexts[0].key,
			Value: move.nexts[0].value,
		})
	}

	return kvs
}

// 跳表数据量大小，单位 byte
func (s *Skiplist) Size() int {
	return s.size
}

// 跳表 kv 对数量
func (s *Skiplist) EntriesCnt() int {
	return s.entrisCnt
}

// 根据 key 获取跳表中对应节点
func (s *Skiplist) getNode(key []byte) *skipNode {
	move := s.head
	// 层数自高向低，逐层检索
	for level := len(s.head.nexts) - 1; level >= 0; level-- {
		// 持续向右移动，直到右侧为空或者右侧节点 key >= 检索 key
		for move.nexts[level] != nil && bytes.Compare(move.nexts[level].key, key) < 0 {
			move = move.nexts[level]
		}
		// 如果右侧节点 key = 检索 key，则找到目标返回. 否则进入下一层
		if move.nexts[level] != nil && bytes.Equal(move.nexts[level].key, key) {
			return move.nexts[level]
		}
	}

	return nil
}

// roll 出一个节点的高度. 最小为 1，每提高 1 层，概率减少为 1//2
func (s *Skiplist) roll() int {
	var level int
	rander := rand.New(rand.NewSource(time.Now().Unix()))
	for rander.Intn(2) == 1 {
		level++
	}
	return level + 1
}
