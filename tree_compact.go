package golsm

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/xiaoxuxiansheng/golsm/memtable"
)

type memTableCompactItem struct {
	walFile  string
	memTable memtable.MemTable
}

// 运行 compact 协程.
func (t *Tree) compact() {
	for {
		select {
		// 接收到 lsm tree 终止信号，退出协程.
		case <-t.stopc:
			// log
			return
			// 接收到 read-only memtable，需要将其溢写到磁盘成为 level0 层 sstable 文件.
		case memCompactItem := <-t.memCompactC:
			t.compactMemTable(memCompactItem)
			// 接收到 level 层 compact 指令，需要执行 level~level+1 之间的 level sorted merge 流程.
		case level := <-t.levelCompactC:
			t.compactLevel(level)
		}
	}
}

func (t *Tree) compactLevel(level int) {
	// 获取到 level 和 level + 1 层需要进行归并的节点
	// 随着 index 递增，数据实时性递增
	pickedNodes := t.pickCompactNodes(level)

	// 执行归并排序，得到多个节点，插入到下一层
	seq := t.levelToSeq[level+1].Load() + 1
	sstWriter, _ := NewSSTWriter(t.sstFile(level+1, seq), t.conf)
	defer sstWriter.Close()

	sstLimit := t.conf.SSTSize * uint64(math.Pow10(level+1))
	pickedKVs := t.pickedNodesToKVs(pickedNodes)
	for i := 0; i < len(pickedKVs); i++ {
		if sstWriter.Size() > sstLimit {
			size, blockToFilter, index := sstWriter.Finish()
			t.insertNode(level+1, seq, size, blockToFilter, index)
			seq = t.levelToSeq[level+1].Load() + 1
			sstWriter, _ = NewSSTWriter(t.sstFile(level+1, seq), t.conf)
			defer sstWriter.Close()
		}

		sstWriter.Append(pickedKVs[i].Key, pickedKVs[i].Value)
		if i == len(pickedKVs)-1 {
			size, blockToFilter, index := sstWriter.Finish()
			t.insertNode(level+1, seq, size, blockToFilter, index)
		}
	}

	// 移除这部分被合并的节点
	t.removeNodes(level, pickedNodes)

	// 尝试触发下一层的 compact 操作
	t.tryTriggerCompact(level + 1)
}

func (t *Tree) pickCompactNodes(level int) []*Node {
	// 每次合并范围为前一半
	startKey := t.nodes[level][0].Start()
	endKey := t.nodes[level][0].End()

	mid := len(t.nodes[level]) >> 1
	if bytes.Compare(t.nodes[level][mid].Start(), startKey) < 0 {
		startKey = t.nodes[level][mid].Start()
	}

	if bytes.Compare(t.nodes[level][mid].End(), endKey) > 0 {
		endKey = t.nodes[level][mid].End()
	}

	var pickedNodes []*Node
	// [start,end] 和 level + 1 层有重叠范围的节点进行合并,得到新的 [start,end]
	for i := level + 1; i >= level; i-- {
		for j := 0; j < len(t.nodes[i]); j++ {
			if bytes.Compare(endKey, t.nodes[i][j].Start()) < 0 || bytes.Compare(startKey, t.nodes[i][j].End()) > 0 {
				continue
			}

			if bytes.Compare(t.nodes[i][j].Start(), startKey) < 0 {
				startKey = t.nodes[i][j].Start()
			}

			if bytes.Compare(t.nodes[i][j].End(), endKey) > 0 {
				endKey = t.nodes[i][j].End()
			}

			pickedNodes = append(pickedNodes, t.nodes[i][j])
		}
	}

	return pickedNodes
}

func (t *Tree) pickedNodesToKVs(pickedNodes []*Node) []*KV {
	// index 越小，数据越久. 所以大 index 数据覆盖小 index 数据
	memtable := t.conf.MemTableConstructor()
	for _, node := range pickedNodes {
		kvs, _ := node.GetAll()
		for _, kv := range kvs {
			memtable.Put(kv.Key, kv.Value)
		}
	}

	_kvs := memtable.All()
	kvs := make([]*KV, 0, len(_kvs))
	for _, kv := range _kvs {
		kvs = append(kvs, &KV{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}

	return kvs
}

func (t *Tree) removeNodes(level int, nodes []*Node) {
outer:
	for k := 0; k < len(nodes); k++ {
		node := nodes[k]
		for i := level + 1; i >= level; i-- {
			for j := 0; j < len(t.nodes[i]); j++ {
				if node != t.nodes[i][j] {
					continue
				}

				t.levelLocks[i].Lock()
				t.nodes[i] = append(t.nodes[i][:j], t.nodes[i][j+1:]...)
				t.levelLocks[i].Unlock()
				continue outer
			}
		}
	}

	go func() {
		for _, node := range nodes {
			// 销毁节点
			node.Destory()
		}
	}()
}

// 将只读 memtable 溢写落盘成为 level0 层 sstable 文件
func (t *Tree) compactMemTable(memCompactItem *memTableCompactItem) {
	// 处理 memtable 溢写工作:
	// 1 memtable 溢写到 0 层 sstable 中
	t.flushMemTable(memCompactItem.memTable)

	// 2 从 rOnly slice 中回收对应的 table
	t.dataLock.Lock()
	for i := 0; i < len(t.rOnlyMemTable); i++ {
		if t.rOnlyMemTable[i].memTable != memCompactItem.memTable {
			continue
		}
		t.rOnlyMemTable = t.rOnlyMemTable[i+1:]
	}
	t.dataLock.Unlock()

	// 3 删除相应的预写日志. 因为 memtable 落盘后数据已经安全，不存在丢失风险
	_ = os.Remove(memCompactItem.walFile)
}

func (t *Tree) flushMemTable(memTable memtable.MemTable) {
	// memtable 写到 level 0 层 sstable 中
	seq := t.levelToSeq[0].Load() + 1

	// 创建 sst writer
	sstWriter, _ := NewSSTWriter(t.sstFile(0, seq), t.conf)
	defer sstWriter.Close()

	// 遍历 memtable 写入数据到 sst writer
	for _, kv := range memTable.All() {
		sstWriter.Append(kv.Key, kv.Value)
	}

	// sstable 落盘
	size, blockToFilter, index := sstWriter.Finish()

	// 构造节点添加到 tree 的 node 中
	t.insertNode(0, seq, size, blockToFilter, index)
	// 尝试引发一轮 compact 操作
	t.tryTriggerCompact(0)
}

func (t *Tree) tryTriggerCompact(level int) {
	// 最后一层不执行 compact 操作
	if level == len(t.nodes)-1 {
		return
	}

	var size uint64
	for _, node := range t.nodes[level] {
		size += node.size
	}

	if size <= t.conf.SSTSize*uint64(math.Pow10(level))*uint64(t.conf.SSTNumPerLevel) {
		return
	}

	go func() {
		t.levelCompactC <- level
	}()
}

func (t *Tree) insertNode(level int, seq int32, size uint64, blockToFilter map[uint64][]byte, index []*Index) {
	file := t.sstFile(level, seq)
	sstReader, _ := NewSSTReader(file, t.conf)

	t.levelToSeq[level].Store(seq)

	newNode := NewNode(t.conf, file, sstReader, level, seq, size, blockToFilter, index)
	if level == 0 {
		t.levelLocks[0].Lock()
		t.nodes[level] = append(t.nodes[level], newNode)
		t.levelLocks[0].Unlock()
		return
	}

	// 更底层，需要根据 key 大小进行插入
	for i := 0; i < len(t.nodes[level])-1; i++ {
		if bytes.Compare(newNode.End(), t.nodes[level][i+1].Start()) < 0 {
			t.levelLocks[level].Lock()
			t.nodes[level] = append(t.nodes[level][:i+1], t.nodes[level][i:]...)
			t.nodes[level][i+1] = newNode
			t.levelLocks[level].Unlock()
			return
		}
	}

	t.levelLocks[level].Lock()
	t.nodes[level] = append(t.nodes[level], newNode)
	t.levelLocks[level].Unlock()
}

func (t *Tree) sstFile(level int, seq int32) string {
	return fmt.Sprintf("%d_%d.sst", level, seq)
}

func (t *Tree) walFile() string {
	return path.Join(t.conf.Dir, "walfile", fmt.Sprintf("%d.wal", t.memTableIndex))
}

func walFileToMemTableIndex(walFile string) int {
	rawIndex := strings.Replace(walFile, ".wal", "", -1)
	index, _ := strconv.Atoi(rawIndex)
	return index
}
