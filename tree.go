package golsm

// 1 构造一棵树，基于 config 与磁盘文件映射
// 2 写入一笔数据
// 3 查询一笔数据
type Tree struct{}

func NewTree() *Tree {
	return &Tree{}
}

func (t *Tree) Put(key, val []byte) {

}

func (t *Tree) Get(key []byte) []byte {
	return nil
}
