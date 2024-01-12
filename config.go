package golsm

import (
	"io/fs"
	"os"
	"path"
	"strings"

	"github.com/xiaoxuxiansheng/golsm/filter"
	"github.com/xiaoxuxiansheng/golsm/memtable"
)

// lsm tree 配置项聚合
type Config struct {
	Dir      string // sst 文件存放的目录
	MaxLevel int    // lsm tree 总共多少层

	// sst 相关
	SSTSize          uint64 // 每个 sst table 大小，默认 4M
	SSTNumPerLevel   int    // 每层多少个 sstable，默认 10 个
	SSTDataBlockSize int    // sst table 中 block 大小 默认 16KB
	SSTFooterSize    int    // sst table 中 footer 部分大小. 固定为 32B

	Filter              filter.Filter                // 过滤器. 默认使用布隆过滤器
	MemTableConstructor memtable.MemTableConstructor // memtable 构造器，默认为跳表
}

// 配置文件构造器.
func NewConfig(dir string, opts ...ConfigOption) (*Config, error) {
	c := Config{
		Dir:           dir, // sstable 文件所在的目录路径
		SSTFooterSize: 32,  // 对应 4 个 uint64，共 32 byte
	}

	// 加载配置项
	for _, opt := range opts {
		opt(&c)
	}

	// 兜底修复
	repaire(&c)

	return &c, c.check() // 校验一下配置是否合法，主要是 check 存放 sst 文件和 wal 文件的目录，如果有缺失则进行目录创建
}

// 校验一下配置是否合法，主要是 check 存放 sst 文件和 wal 文件的目录，如果有缺失则进行目录创建
func (c *Config) check() error {
	// sstable 文件目录确保存在
	if _, err := os.ReadDir(c.Dir); err != nil {
		_, ok := err.(*fs.PathError)
		if !ok || !strings.HasSuffix(err.Error(), "no such file or directory") {
			return err
		}
		if err = os.Mkdir(c.Dir, os.ModePerm); err != nil {
			return err
		}
	}

	// wal 文件目录确保存在
	walDir := path.Join(c.Dir, "walfile")
	if _, err := os.ReadDir(walDir); err != nil {
		_, ok := err.(*fs.PathError)
		if !ok || !strings.HasSuffix(err.Error(), "no such file or directory") {
			return err
		}
		if err = os.Mkdir(walDir, os.ModePerm); err != nil {
			return err
		}
	}

	return nil
}

// 配置项
type ConfigOption func(*Config)

// lsm tree 最大层数. 默认为 7 层.
func WithMaxLevel(maxLevel int) ConfigOption {
	return func(c *Config) {
		c.MaxLevel = maxLevel
	}
}

// level0层每个 sstable 文件的大小，单位 byte. 默认为 1 MB.
// 且每加深一层，sstable 文件大小限制阈值放大 10 倍.
func WithSSTSize(sstSize uint64) ConfigOption {
	return func(c *Config) {
		c.SSTSize = sstSize
	}
}

// sstable 中每个 block 块的大小限制. 默认为 16KB.
func WithSSTDataBlockSize(sstDataBlockSize int) ConfigOption {
	return func(c *Config) {
		c.SSTDataBlockSize = sstDataBlockSize
	}
}

// 每个 level 层预期最多存放的 sstable 文件个数. 默认为 10 个.
func WithSSTNumPerLevel(sstNumPerLevel int) ConfigOption {
	return func(c *Config) {
		c.SSTNumPerLevel = sstNumPerLevel
	}
}

// 注入过滤器的具体实现. 默认使用本项目下实现的布隆过滤器 bloom filter.
func WithFilter(filter filter.Filter) ConfigOption {
	return func(c *Config) {
		c.Filter = filter
	}
}

// 注入有序表构造器. 默认使用本项目下实现的跳表 skiplist.
func WithMemtableConstructor(memtableConstructor memtable.MemTableConstructor) ConfigOption {
	return func(c *Config) {
		c.MemTableConstructor = memtableConstructor
	}
}

func repaire(c *Config) {
	// lsm tree 默认为 7 层.
	if c.MaxLevel <= 1 {
		c.MaxLevel = 7
	}

	// level0 层每个 sstable 文件默认大小限制为 1MB.
	// 且每加深一层，sstable 文件大小限制阈值放大 10 倍.
	if c.SSTSize <= 0 {
		c.SSTSize = 1024 * 1024
	}

	// sstable 中每个 block 块的大小限制. 默认为 16KB.
	if c.SSTDataBlockSize <= 0 {
		c.SSTDataBlockSize = 16 * 1024 // 16KB
	}

	// 每个 level 层预期最多存放的 sstable 文件个数. 默认为 10 个.
	if c.SSTNumPerLevel <= 0 {
		c.SSTNumPerLevel = 10
	}

	// 注入过滤器的具体实现. 默认使用本项目下实现的布隆过滤器 bloom filter.
	if c.Filter == nil {
		c.Filter, _ = filter.NewBloomFilter(1024)
	}

	// 注入有序表构造器. 默认使用本项目下实现的跳表 skiplist.
	if c.MemTableConstructor == nil {
		c.MemTableConstructor = memtable.NewSkiplist
	}
}
