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

// 针对 level 层进行排序归并操作
func (t *Tree) compactLevel(level int) {
	// 获取到 level 和 level + 1 层内需要进行本次归并的节点
	pickedNodes := t.pickCompactNodes(level)

	// 插入到 level + 1 层对应的目标 sstWriter
	seq := t.levelToSeq[level+1].Load() + 1
	sstWriter, _ := NewSSTWriter(t.sstFile(level+1, seq), t.conf)
	defer sstWriter.Close()

	// 获取 level + 1 层每个 sst 文件的大小阈值
	sstLimit := t.conf.SSTSize * uint64(math.Pow10(level+1))
	// 获取本次排序归并的节点涉及到的所有 kv 数据
	pickedKVs := t.pickedNodesToKVs(pickedNodes)
	// 遍历每笔需要归并的 kv 数据
	for i := 0; i < len(pickedKVs); i++ {
		// 倘若新生成的 level + 1 层 sst 文件大小已经超限
		if sstWriter.Size() > sstLimit {
			// 将 sst 文件溢写落盘
			size, blockToFilter, index := sstWriter.Finish()
			// 将 sst 文件对应 node 插入到 lsm tree 内存结构中
			t.insertNode(level+1, seq, size, blockToFilter, index)
			// 构造一个新的 level + 1 层 sstWriter
			seq = t.levelToSeq[level+1].Load() + 1
			sstWriter, _ = NewSSTWriter(t.sstFile(level+1, seq), t.conf)
			defer sstWriter.Close()
		}

		// 将 kv 数据追加到 sstWriter
		sstWriter.Append(pickedKVs[i].Key, pickedKVs[i].Value)
		// 倘若这是最后一笔 kv 数据，需要负责把 sstWriter 溢写落盘并把对应 node 插入到 lsm tree 内存结构中
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

// 获取本轮 compact 流程涉及到的所有节点，范围涵盖 level 和 level+1 层
func (t *Tree) pickCompactNodes(level int) []*Node {
	// 每次合并范围为当前层前一半节点
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
	// 将 level 层和 level + 1 层 和 [start,end] 范围有重叠的节点进行合并
	for i := level + 1; i >= level; i-- {
		for j := 0; j < len(t.nodes[i]); j++ {
			if bytes.Compare(endKey, t.nodes[i][j].Start()) < 0 || bytes.Compare(startKey, t.nodes[i][j].End()) > 0 {
				continue
			}

			// 所有范围有重叠的节点都追加到 list
			pickedNodes = append(pickedNodes, t.nodes[i][j])
		}
	}

	return pickedNodes
}

// 获取本轮 compact 流程涉及到的所有 kv 对. 这个过程中可能存在重复 k，保证只保留最新的 v
func (t *Tree) pickedNodesToKVs(pickedNodes []*Node) []*KV {
	// index 越小，数据越老. index 越大，数据越新
	// 所以使用大 index 的数据覆盖小 index 数据，以久覆新
	memtable := t.conf.MemTableConstructor()
	for _, node := range pickedNodes {
		kvs, _ := node.GetAll()
		for _, kv := range kvs {
			memtable.Put(kv.Key, kv.Value)
		}
	}

	// 借助 memtable 实现有序排列
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

// 移除所有完成 compact 流程的老节点
func (t *Tree) removeNodes(level int, nodes []*Node) {
	// 从 lsm tree 的 nodes 中移除老节点
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
		// 销毁老节点，包括关闭 sst reader，并且删除节点对应 sst 磁盘文件
		for _, node := range nodes {
			node.Destroy()
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

// 将 memtable 的数据溢写落盘到 level0 层成为一个新的 sst 文件
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

// 插入一个 node 到指定 level 层
func (t *Tree) insertNodeWithReader(sstReader *SSTReader, level int, seq int32, size uint64, blockToFilter map[uint64][]byte, index []*Index) {
	file := t.sstFile(level, seq)
	// 记录当前 level 层对应的 seq 号（单调递增）
	t.levelToSeq[level].Store(seq)

	// 创建一个 lsm node
	newNode := NewNode(t.conf, file, sstReader, level, seq, size, blockToFilter, index)
	// 对于 level0 而言，只需要 append 插入 node 即可
	if level == 0 {
		t.levelLocks[0].Lock()
		t.nodes[level] = append(t.nodes[level], newNode)
		t.levelLocks[0].Unlock()
		return
	}

	// 对于 level1~levelk 层，需要根据 node 中 key 的大小，遵循顺序插入
	for i := 0; i < len(t.nodes[level])-1; i++ {
		// 遵循从小到大的遍历顺序，找到首个最小 key 比 newNode 最大 key 还大的 node，将 newNode 插入在其之前
		if bytes.Compare(newNode.End(), t.nodes[level][i+1].Start()) < 0 {
			t.levelLocks[level].Lock()
			t.nodes[level] = append(t.nodes[level][:i+1], t.nodes[level][i:]...)
			t.nodes[level][i+1] = newNode
			t.levelLocks[level].Unlock()
			return
		}
	}

	// 遍历完 level 层所有节点都还没插入 newNode，说明 newNode 是该层 key 值最大的节点，则 append 到最后即可
	t.levelLocks[level].Lock()
	t.nodes[level] = append(t.nodes[level], newNode)
	t.levelLocks[level].Unlock()
}

func (t *Tree) insertNode(level int, seq int32, size uint64, blockToFilter map[uint64][]byte, index []*Index) {
	file := t.sstFile(level, seq)
	sstReader, _ := NewSSTReader(file, t.conf)

	t.insertNodeWithReader(sstReader, level, seq, size, blockToFilter, index)
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
