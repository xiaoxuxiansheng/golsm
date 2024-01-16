package filter

import (
	"errors"

	"github.com/spaolacci/murmur3"
)

// 布隆过滤器
type BloomFilter struct {
	m          int      // bitmap 的长度，单位 bit
	hashedKeys []uint32 // 添加到布隆过滤器的一系列 key 的 hash 值
}

// 布隆过滤器构造器
func NewBloomFilter(m int) (*BloomFilter, error) {
	if m <= 0 {
		return nil, errors.New("m must be postive")
	}
	return &BloomFilter{
		m: m,
	}, nil
}

// 添加一个 key 到布隆过滤器
func (bf *BloomFilter) Add(key []byte) {
	bf.hashedKeys = append(bf.hashedKeys, murmur3.Sum32(key))
}

// 判断过滤器中是否存在 key（注意，可能存在假阳性误判问题）
func (bf *BloomFilter) Exist(bitmap, key []byte) bool {
	// 生成 bitmap 时，需要把哈希函数个数 k 的值设置在 bitmap 的最后一个 byte 上
	if bitmap == nil {
		bitmap = bf.Hash()
	}
	// 获取hash 函数的个数 k
	k := bitmap[len(bitmap)-1]

	// 第一个基准 hash 函数 h1 = murmur3.Sum32
	// 第二个基准 hash 函数 h2 = h1 >> 17 | h2 << 15
	// 之后所有使用的 hash 函数均通过 h1 和 h2 线性无关的组合生成
	// 第 i 个 hash 函数 gi = h1 + i * h2

	// h1
	hashedKey := murmur3.Sum32(key)
	// h2
	delta := (hashedKey >> 17) | (hashedKey << 15)
	for i := uint32(0); i < uint32(k); i++ {
		// gi = h1 + i * h2
		targetBit := (hashedKey + i*delta) % uint32(len(bitmap)<<3)
		// 找到对应的 bit 位，如果值为 1，则继续判断；如果值为 0，则 key 肯定不存在
		if bitmap[targetBit>>3]&(1<<(targetBit&7)) == 0 {
			return false
		}
	}

	// key 映射的所有 bit 位均为 1，则认为 key 存在（存在误判概率）
	return true
}

// 生成过滤器对应的 bitmap. 最后一个 byte 标识 k 的数值
func (bf *BloomFilter) Hash() []byte {
	// k: 根据 m 和 n 推导出最佳 hash 函数个数
	k := bf.bestK()
	// 获取出一个空的 bitmap，最后一个 byte 位值设置为 k
	bitmap := bf.bitmap(k)

	// 第一个基准 hash 函数 h1 = murmur3.Sum32
	// 第二个基准 hash 函数 h2 = h1 >> 17 | h2 << 15
	// 之后所有使用的 hash 函数均通过 h1 和 h2 线性无关的组合生成
	// 第 i 个 hash 函数 gi = h1 + i * h2
	for _, hashedKey := range bf.hashedKeys {
		// hashedKey 为 h1
		// delta 为 h2
		delta := (hashedKey >> 17) | (hashedKey << 15)
		for i := uint32(0); i < uint32(k); i++ {
			// 第 i 个 hash 函数 gi = h1 + i * h2
			// 需要标记为 1 的 bit 位
			targetBit := (hashedKey + i*delta) % uint32(len(bitmap)<<3)
			bitmap[targetBit>>3] |= (1 << (targetBit & 7))
		}
	}

	return bitmap
}

// 重置过滤器
func (bf *BloomFilter) Reset() {
	bf.hashedKeys = bf.hashedKeys[:0]
}

// 获取过滤器中存在的 key 个数
func (bf *BloomFilter) KeyLen() int {
	return len(bf.hashedKeys)
}

// 生成一个空的 bitmap
func (bf *BloomFilter) bitmap(k uint8) []byte {
	// bytes = bits / 8 (向上取整)
	bitmapLen := (bf.m + 7) >> 3
	bitmap := make([]byte, bitmapLen+1)
	// 最后一位标识 k 的信息
	bitmap[bitmapLen] = k
	return bitmap
}

// 根据 m 和 n 推算出最佳的 k
func (bf *BloomFilter) bestK() uint8 {
	// k 最佳计算公式：k = ln2 * m / n  m——bitmap 长度 n——key个数
	k := uint8(69 * bf.m / 100 / len(bf.hashedKeys))
	// k ∈ [1,30]
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return k
}
