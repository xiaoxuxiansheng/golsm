package filter

import (
	"errors"

	"github.com/spaolacci/murmur3"
)

type BloomFilter struct {
	m          int      // bitmap 的长度
	hashedKeys []uint32 // 一系列 key 的 hash 值
}

func NewBloomFilter(m int) (*BloomFilter, error) {
	if m <= 0 {
		return nil, errors.New("m must be postive")
	}
	return &BloomFilter{
		m: m,
	}, nil
}

// 添加 key 到过滤器
func (bf *BloomFilter) Add(key []byte) {
	bf.hashedKeys = append(bf.hashedKeys, murmur3.Sum32(key))
}

// 过滤器中是否存在 key
func (bf *BloomFilter) Exist(bitmap, key []byte) bool {
	if bitmap == nil {
		bitmap = bf.Hash()
	}
	k := bitmap[len(bitmap)-1]

	hashedKey := murmur3.Sum32(key)
	delta := (hashedKey >> 17) | (hashedKey << 15)
	for i := uint32(0); i < uint32(k); i++ {
		targetBit := (hashedKey + delta) % uint32(len(bitmap)<<3)
		if bitmap[targetBit>>3]&(1<<(targetBit&7)) == 0 {
			return false
		}
	}

	return true
}

// 生成过滤器对应的 bitmap. 最后一个 byte 标识 k 的数值
func (bf *BloomFilter) Hash() []byte {
	// k: hash 函数个数
	k := bf.bestK()

	bitmap := bf.bitmap(k)

	// 遍历每个 hash key，对 bitmap 进行标记
	// h1 = murmur3
	// h2 = ( h1 >> 17 ) | ( h1 << 15 )
	// hashk = h1 + k * h2
	for _, hashedKey := range bf.hashedKeys {
		delta := (hashedKey >> 17) | (hashedKey << 15)
		for i := uint32(0); i < uint32(k); i++ {
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

func (bf *BloomFilter) KeyLen() int {
	return len(bf.hashedKeys)
}

func (bf *BloomFilter) bitmap(k uint8) []byte {
	// bytes = bits / 8 (向上取整)
	bitmapLen := (bf.m + 7) >> 3
	bitmap := make([]byte, bitmapLen+1)
	// 最后一位标识 k 的信息
	bitmap[bitmapLen] = k
	return bitmap
}

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
