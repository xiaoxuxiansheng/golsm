package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SharedPrefixLen(t *testing.T) {
	assert.Equal(t, SharedPrefixLen([]byte("a"), nil), 0)
	assert.Equal(t, SharedPrefixLen([]byte("ab"), []byte("abc")), 2)
	assert.Equal(t, SharedPrefixLen([]byte("ab"), []byte("c")), 0)
}

func Test_GetSeparatorBetween(t *testing.T) {
	assert.Equal(t, GetSeparatorBetween(nil, []byte("b")), []byte("a"))
	assert.Equal(t, GetSeparatorBetween([]byte("abcd"), []byte("abcde")), []byte("abcd"))
	assert.Equal(t, GetSeparatorBetween([]byte("abcd"), []byte("abce")), []byte("abcd"))
}
