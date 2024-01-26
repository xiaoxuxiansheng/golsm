package golsm

import (
	"io/fs"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/xiaoxuxiansheng/golsm/wal"
)

// 读取 sst 文件，还原出整棵树
func (t *Tree) constructTree() error {
	// 读取 sst 文件目录下的 sst 文件列表
	sstEntries, err := t.getSortedSSTEntries()
	if err != nil {
		return err
	}

	// 遍历每个 sst 文件，将其加载为 node 添加 lsm tree 的 nodes 内存切片中
	for _, sstEntry := range sstEntries {
		if err = t.loadNode(sstEntry); err != nil {
			return err
		}
	}

	return nil
}

func (t *Tree) getSortedSSTEntries() ([]fs.DirEntry, error) {
	allEntries, err := os.ReadDir(t.conf.Dir)
	if err != nil {
		return nil, err
	}

	sstEntries := make([]fs.DirEntry, 0, len(allEntries))
	for _, entry := range allEntries {
		if entry.IsDir() {
			continue
		}

		if !strings.HasSuffix(entry.Name(), ".sst") {
			continue
		}

		sstEntries = append(sstEntries, entry)
	}

	sort.Slice(sstEntries, func(i, j int) bool {
		levelI, seqI := getLevelSeqFromSSTFile(sstEntries[i].Name())
		levelJ, seqJ := getLevelSeqFromSSTFile(sstEntries[j].Name())
		if levelI == levelJ {
			return seqI < seqJ
		}
		return levelI < levelJ
	})
	return sstEntries, nil
}

// 将一个 sst 文件作为一个 node 加载进入 lsm tree 的拓扑结构中
func (t *Tree) loadNode(sstEntry fs.DirEntry) error {
	// 创建 sst 文件对应的 reader
	sstReader, err := NewSSTReader(sstEntry.Name(), t.conf)
	if err != nil {
		return err
	}

	// 读取各 block 块对应的 filter 信息
	blockToFilter, err := sstReader.ReadFilter()
	if err != nil {
		return err
	}

	// 读取 index 信息
	index, err := sstReader.ReadIndex()
	if err != nil {
		return err
	}

	// 获取 sst 文件的大小，单位 byte
	size, err := sstReader.Size()
	if err != nil {
		return err
	}

	// 解析 sst 文件名，得知 sst 文件对应的 level 以及 seq 号
	level, seq := getLevelSeqFromSSTFile(sstEntry.Name())
	// 将 sst 文件作为一个 node 插入到 lsm tree 中
	t.insertNodeWithReader(sstReader, level, seq, size, blockToFilter, index)
	return nil
}

func getLevelSeqFromSSTFile(file string) (level int, seq int32) {
	file = strings.Replace(file, ".sst", "", -1)
	splitted := strings.Split(file, "_")
	level, _ = strconv.Atoi(splitted[0])
	_seq, _ := strconv.Atoi(splitted[1])
	return level, int32(_seq)
}

// 读取 wal 还原出 memtable
func (t *Tree) constructMemtable() error {
	// 1 读 wal 目录，获取所有的 wal 文件
	raw, _ := os.ReadDir(path.Join(t.conf.Dir, "walfile"))

	// 2 wal 文件除杂
	var wals []fs.DirEntry
	for _, entry := range raw {
		if entry.IsDir() {
			continue
		}

		// 要求文件必须为 .wal 类型
		if !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}

		wals = append(wals, entry)
	}

	// 3 倘若 wal 目录不存在或者 wal 文件不存在，则构造一个新的 memtable
	if len(wals) == 0 {
		t.newMemTable()
		return nil
	}

	// 4 依次还原 memtable. 最晚一个 memtable 作为读写 memtable
	// 前置 memtable 作为只读 memtable，分别添加到内存 slice 和 channel 中.
	return t.restoreMemTable(wals)
}

// 基于 wal 文件还原出一系列只读 memtable 和唯一一个读写 memtable
func (t *Tree) restoreMemTable(wals []fs.DirEntry) error {
	// 1 wal 排序，index 单调递增，数据实时性也随之单调递增
	sort.Slice(wals, func(i, j int) bool {
		indexI := walFileToMemTableIndex(wals[i].Name())
		indexJ := walFileToMemTableIndex(wals[j].Name())
		return indexI < indexJ
	})

	// 2 依次还原 memtable，添加到内存和 channel
	for i := 0; i < len(wals); i++ {
		name := wals[i].Name()
		file := path.Join(t.conf.Dir, "walfile", name)

		// 构建与 wal 文件对应的 walReader
		walReader, err := wal.NewWALReader(file)
		if err != nil {
			return err
		}
		defer walReader.Close()

		// 通过 reader 读取 wal 文件内容，将数据注入到 memtable 中
		memtable := t.conf.MemTableConstructor()
		if err = walReader.RestoreToMemtable(memtable); err != nil {
			return err
		}

		if i == len(wals)-1 { // 倘若是最后一个 wal 文件，则 memtable 作为读写 memtable
			t.memTable = memtable
			t.memTableIndex = walFileToMemTableIndex(name)
			t.walWriter, _ = wal.NewWALWriter(file)
		} else { // memtable 作为只读 memtable，需要追加到只读 slice 以及 channel 中，继续推进完成溢写落盘流程
			memTableCompactItem := memTableCompactItem{
				walFile:  file,
				memTable: memtable,
			}

			t.rOnlyMemTable = append(t.rOnlyMemTable, &memTableCompactItem)
			t.memCompactC <- &memTableCompactItem
		}
	}
	return nil
}
