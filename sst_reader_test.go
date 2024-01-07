package golsm

import (
	"fmt"
	"testing"
)

func Test_SSTReader(t *testing.T) {
	// 构造一个 sst writer 写入数据
	conf := NewConfig("./", WithSSTDataBlockSize(16))
	sstWriter, err := NewSSTWriter("test_write_read.sst", conf)
	if err != nil {
		t.Error(err)
		return
	}
	defer sstWriter.Close()

	// datablock1: record: [0 1 1 a b] [1 1 2 b c d] [0 1 1 e f]
	// datablock2: record: [0 2 1 e f g h]
	// filter: 0 -> bitmap1  16 -> bitmap2
	// index: [` 0 0] [e 0 16] [ef 16 7]
	// footer: ...
	expectkvs := []*KV{
		{
			Key:   []byte("a"),
			Value: []byte("b"),
		},
		{
			Key:   []byte("ab"),
			Value: []byte("cd"),
		},
		{
			Key:   []byte("e"),
			Value: []byte("f"),
		},
		{
			Key:   []byte("ef"),
			Value: []byte("gh"),
		},
	}

	for _, kv := range expectkvs {
		sstWriter.Append(kv.Key, kv.Value)
	}

	_, expectBlockToFilter, expectIndex := sstWriter.Finish()

	// 构造一个 sst reader 读取数据
	sstReader, err := NewSSTReader("test_write_read.sst", conf)
	if err != nil {
		t.Error(err)
		return
	}
	defer sstReader.Close()

	gotBlockToFilter, err := sstReader.ReadFilter()
	if err != nil {
		t.Error(err)
		return
	}

	gotIndex, err := sstReader.ReadIndex()
	if err != nil {
		t.Error(err)
		return
	}

	if err = assertFilterEqual(expectBlockToFilter, gotBlockToFilter); err != nil {
		t.Error(err)
		return
	}

	if err = assertIndexEqual(expectIndex, gotIndex); err != nil {
		t.Error(err)
		return
	}

	gotKVs, err := sstReader.ReadData()
	if err != nil {
		t.Error(err)
		return
	}

	if err = assertDataEqual(expectkvs, gotKVs); err != nil {
		t.Error(err)
	}
}

func assertFilterEqual(expect, got map[uint64][]byte) error {
	if len(expect) != len(got) {
		return fmt.Errorf("expect len: %d, got len: %d", len(expect), len(got))
	}

	for expectK, expectV := range expect {
		gotV := got[expectK]
		if string(expectV) != string(gotV) {
			return fmt.Errorf("key: %d, expect v: %s, got v: %s", expectK, expectV, gotV)
		}
	}

	return nil
}

func assertIndexEqual(expect, got []*Index) error {
	if len(expect) != len(got) {
		return fmt.Errorf("expect len: %d, got len: %d", len(expect), len(got))
	}

	for i := 0; i < len(expect); i++ {
		if string(expect[i].Key) != string(got[i].Key) {
			return fmt.Errorf("index: %d, expect key: %s, got key: %s", i, expect[i].Key, got[i].Key)
		}

		if expect[i].PrevBlockOffset != got[i].PrevBlockOffset {
			return fmt.Errorf("index: %d, expect offset: %d, got offset: %d", i, expect[i].PrevBlockOffset, got[i].PrevBlockOffset)
		}

		if expect[i].PrevBlockSize != got[i].PrevBlockSize {
			return fmt.Errorf("index: %d, expect size: %d, got size: %d", i, expect[i].PrevBlockSize, got[i].PrevBlockSize)
		}
	}
	return nil
}

func assertDataEqual(expect, got []*KV) error {
	if len(expect) != len(got) {
		return fmt.Errorf("expect len: %d, got len: %d", len(expect), len(got))
	}

	for i := 0; i < len(expect); i++ {
		if string(expect[i].Key) != string(got[i].Key) {
			return fmt.Errorf("data: %d, data key: %s, got key: %s", i, expect[i].Key, got[i].Key)
		}

		if string(expect[i].Value) != string(got[i].Value) {
			return fmt.Errorf("data: %d, expect offset: %d, got offset: %d", i, expect[i].Value, got[i].Value)
		}

	}
	return nil
}
