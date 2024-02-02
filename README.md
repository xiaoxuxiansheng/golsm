<p align="center">
<img src="https://github.com/xiaoxuxiansheng/golsm/blob/main/img/golsm_page.png" />
<b>golsm: 基于 go 语言实现的 lsm tree</b>
<br/><br/>
</p>

## 📚 前言
笔者在学习 lsm tree 实现方案的过程中，在很大程度上借鉴了 simple-raft 项目，在此特别致敬一下作者.
附上传送门：https://github.com/nananatsu/simple-raft/tree/master/pkg/lsm

## 📖 简介
100% 纯度 go 语言实现的 lsm tree 框架，能够更好地应对组织写密集型 kv 存储结构.

## 💡 `lsm tree` 技术原理及源码实现
<a href="https://mp.weixin.qq.com/s?__biz=MzkxMjQzMjA0OQ==&mid=2247484182&idx=1&sn=6ec38965bc927bf72eee567342f6376a">原理篇：初探 rocksDB 之 lsm tree</a> <br/><br/>
<a href="https://mp.weixin.qq.com/s?__biz=MzkxMjQzMjA0OQ==&mid=2247484876&idx=1&sn=8514ad4dc0cc63bc5d193a194e81d7b6">实现篇一：基于go实现lsm tree 之主干框架</a> <br/><br/>
<a href="https://mp.weixin.qq.com/s?__biz=MzkxMjQzMjA0OQ==&mid=2247484901&idx=1&sn=4177bd9d50a7e4ce3dddf12dec949006">实现篇二：基于go实现lsm tree之memtable结构</a> <br/><br/>
<a href="https://mp.weixin.qq.com/s?__biz=MzkxMjQzMjA0OQ==&mid=2247484944&idx=1&sn=59cfa74943df0f3c5d597656b7c0b4e9">实现篇三：基于go实现lsm tree之sstable结构</a> <br/><br/>
<a href="https://mp.weixin.qq.com/s?__biz=MzkxMjQzMjA0OQ==&mid=2247484945&idx=1&sn=b1553b217da8b7695af1967617a1ce16">实现篇四：基于go实现lsm tree之level sorted merge流程</a>

## 🖥 使用示例
```go
func Test_LSM_UseCase(t *testing.T) {
	// 1 构造配置文件
	conf, _ := NewConfig("./lsm", // lsm sstable 文件的存放目录
		WithMaxLevel(7),           // 7层 lsm tree
		WithSSTSize(1024*1024),       // level 0 层，每个 sstable 的大小为 1M
		WithSSTDataBlockSize(16*1024), // sstable 中，每个 block 大小为 16KB
		WithSSTNumPerLevel(10),     // 每个 level 存放 10 个 sstable 文件
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
```
