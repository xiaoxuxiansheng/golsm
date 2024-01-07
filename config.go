package golsm

import "github.com/xiaoxuxiansheng/golsm/filter"

type Config struct {
	Dir                 string
	MaxLevel            int
	SSTSize             int // sst table 大小
	SSTDataBlockSize    int // sst table 中 block 大小
	SSTFooterSize       int // sst table 中 footer 部分大小
	SSTBlockTrailerSize int // sst block 中 trailer 部分大小

	Filter filter.Filter // 过滤器
}

func NewConfig(dir string, opts ...ConfigOption) *Config {
	c := Config{
		Dir:           dir,
		SSTFooterSize: 32, // 4 个 uint64
	}

	for _, opt := range opts {
		opt(&c)
	}

	repaire(&c)
	return &c
}

type ConfigOption func(*Config)

func WithMaxLevel(maxLevel int) ConfigOption {
	return func(c *Config) {
		c.MaxLevel = maxLevel
	}
}

func WithSSTSize(sstSize int) ConfigOption {
	return func(c *Config) {
		c.SSTSize = sstSize
	}
}

func WithSSTDataBlockSize(sstDataBlockSize int) ConfigOption {
	return func(c *Config) {
		c.SSTDataBlockSize = sstDataBlockSize
	}
}

func WithSSTBlockTrailerSize(sstBlockTrailerSize int) ConfigOption {
	return func(c *Config) {
		c.SSTBlockTrailerSize = sstBlockTrailerSize
	}
}

func WithFilter(filter filter.Filter) ConfigOption {
	return func(c *Config) {
		c.Filter = filter
	}
}

func repaire(c *Config) {
	if c.MaxLevel <= 0 {
		c.MaxLevel = 7
	}

	if c.SSTSize <= 0 {
		c.SSTSize = 4096 * 1024 // 4MB
	}

	if c.SSTDataBlockSize <= 0 {
		c.SSTDataBlockSize = 16 * 1024 // 16KB
	}

	if c.SSTBlockTrailerSize <= 0 {
		c.SSTBlockTrailerSize = 4 // 4B
	}
}
