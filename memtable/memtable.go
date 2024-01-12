package memtable

// memtable 构造器
type MemTableConstructor func() MemTable

// 有序表 interface
type MemTable interface {
	Put(key, value []byte)         // 写入数据
	Get(key []byte) ([]byte, bool) // 读取数据，第二个 bool flag 标识数据是否存在
	All() []*KV                    // 返回所有的 kv 对数据
	Size() int                     // 有序表内数据大小，单位 byte
	EntriesCnt() int               // kv 对数量
}

type KV struct {
	Key, Value []byte
}
