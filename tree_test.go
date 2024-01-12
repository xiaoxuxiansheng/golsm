package golsm

import (
	"bytes"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_LSM_UseCase(t *testing.T) {
	// 1 构造配置文件
	conf, _ := NewConfig("./lsm", // lsm sstable 文件的存放目录
		WithMaxLevel(7),           // 7层 lsm tree
		WithSSTSize(2*1024),       // level 0 层，每个 sstable 的大小为 1M
		WithSSTDataBlockSize(512), // sstable 中，每个 block 大小为 16KB
		WithSSTNumPerLevel(2),     // 每个 level 存放 10 个 sstable 文件
	)

	// 2 创建一个 lsm tree 实例
	lsmTree, _ := NewTree(conf)
	defer lsmTree.Close()

	// 3 写入数据
	_ = lsmTree.Put([]byte{1}, []byte{2})

	// 4 读取数据
	v, _, _ := lsmTree.Get([]byte{1})

	t.Log(v)
}

func Test_LSM(t *testing.T) {
	// 构造配置文件
	conf, err := NewConfig("./lsm", // lsm sstable 文件的存放目录
		WithMaxLevel(7),           // 7层 lsm tree
		WithSSTSize(2*1024),       // level 0 层，每个 sstable 的大小为 1M
		WithSSTDataBlockSize(512), // sstable 中，每个 block 大小为 16KB
		WithSSTNumPerLevel(2),     // 每个 level 存放 10 个 sstable 文件
	)
	if err != nil {
		t.Error(err)
		return
	}

	// 创建一个 lsm tree 实例
	lsmTree, err := NewTree(conf)
	if err != nil {
		t.Error(err)
		return
	}
	defer lsmTree.Close()

	kvs := []struct {
		key []byte
		val []byte
	}{}

	for i := 65; i <= 122; i++ {
		for j := 65; j <= 122; j++ {
			kvs = append(kvs, struct {
				key []byte
				val []byte
			}{
				key: []byte{uint8(i), uint8(j)},
				val: []byte{uint8(i), uint8(j)},
			})
		}
	}

	for i := 0; i < len(kvs); i++ {
		if err = lsmTree.Put(kvs[i].key, kvs[i].val); err != nil {
			t.Error(err)
			return
		}
	}

	for _, kv := range kvs {
		v, ok, err := lsmTree.Get(kv.key)
		if err != nil {
			t.Error(err)
			return
		}
		if !ok {
			t.Errorf("key: %s not exist", kv.key)
			return
		}
		if !bytes.Equal(v, kv.val) {
			t.Errorf("key: %s, expect v: %s, got: %s", kv.key, kv.val, v)
			return
		}
	}
}

func Test_Tree_getSortedSSTEntries(t *testing.T) {
	if err := os.Mkdir("test", os.ModePerm); err != nil {
		t.Error(err)
		return
	}
	defer func() {
		if err := os.Remove("./test"); err != nil {
			t.Error(err)
		}
	}()

	files := []string{"1_1.sst", "1_2.ab", "10_0.sst", "2_3.sst", "1_5.sst", "10_10.sst", "10_5.sst"}
	for _, file := range files {
		file := file
		fd, err := os.Create(path.Join("test", file))
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			fd.Close()
			if err = os.Remove(path.Join("test", file)); err != nil {
				t.Error(err)
			}
		}()
	}

	tree := Tree{
		conf: &Config{
			Dir: "./test",
		},
	}

	expectEntries := []string{
		"1_1.sst", "1_5.sst", "2_3.sst", "10_0.sst", "10_5.sst", "10_10.sst",
	}

	gotEntries, err := tree.getSortedSSTEntries()
	if err != nil {
		t.Error(err)
		return
	}

	if len(gotEntries) != len(expectEntries) {
		t.Errorf("got len: %d, expect: %d", len(gotEntries), len(expectEntries))
		return
	}

	for i := 0; i < len(gotEntries); i++ {
		if gotEntries[i].Name() != expectEntries[i] {
			t.Errorf("index: %d, got entries: %s, expect: %s", i, gotEntries[i].Name(), expectEntries[i])
		}
	}
}

func Test_pathJoin(t *testing.T) {
	assert.Equal(t, path.Join("./", "wal", "1.sst"), "wal/1.sst")
	assert.Equal(t, path.Join("/root/", "/wal", "1.sst"), "/root/wal/1.sst")
	assert.Equal(t, path.Join("/root", "/wal", "1.sst"), "/root/wal/1.sst")
	assert.Equal(t, path.Join("/root", "wal", "1.sst"), "/root/wal/1.sst")
}
