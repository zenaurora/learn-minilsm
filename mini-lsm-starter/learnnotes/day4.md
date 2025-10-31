1. 在 SST 中查找某个键的时间复杂度是多少？
首先使用block_meta二分搜索，查找一个合适块: Log(Blocks)
然后如果这个Block没有被缓存，需要进行IO加载到内存中
之后需要在这个块内再进行一次二分查找 Log(Keys)
所以是 O(log B + Log K) = O(log(B*K))

2. 查找一个不存在的键时，光标会停在哪里？
第一个大于key的位置
这两个实现不知道应该使用哪个？
```rust
self.blk_iter = BlockIterator::create_and_seek_to_key(next_block, key);
self.blk_iter = BlockIterator::create_and_seek_to_first(next_block);
```
目前来看当前实现，如果create_and_seek_to_key没有找到，iter的key会被设置为empty
而且iter的valid的检查就是看key是不是empty
进而导致如果一直找不到，一直无效，就会一直向下一个block找

3. 是否可以（或是否有必要）对 SST 文件进行就地更新？
SST是不可变的，不需要更新

4. SST 通常体积较大（例如 256MB）。在这种情况下，复制或扩展 Vec 的开销将非常显著。你的实现是否提前为 SST 构建器分配了足够的空间？你是如何具体实现的？
`data: Vec::with_capacity(block_size),`

5. 查看 moka 块缓存时，为什么它返回的是 Arc<Error> ，而不是原始的 Error 呢？
原本就是一个` pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>>`
如果不这么写会报错：
```rust
cache
    .try_get_with((self.id, block_idx), || self.read_block(block_idx))
    .map_err(|e| anyhow::anyhow!("{}", e))
```

6. 使用 block cache 能否保证内存中最多有固定数量的 blocks？


