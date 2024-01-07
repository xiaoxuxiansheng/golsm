package util

func SharedPrefixLen(a, b []byte) int {
	var i int
	for ; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			break
		}
	}
	return i
}

// 返回结果 x，保证 a <= x < b. 使用方需要自行保证 a < b
func GetSeparatorBetween(a, b []byte) []byte {
	// 倘若 a 为空，则返回一个比 b 小的结果即可
	if len(a) == 0 {
		sepatator := make([]byte, len(b))
		copy(sepatator, b)
		return append(sepatator[:len(b)-1], sepatator[len(b)-1]-1)
	}

	// 返回 a 即可
	return a
}
