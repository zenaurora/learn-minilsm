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
所以我的理解就是tey_get_with就是一个支持并发的一个函数，出现错误的时候可以共享一个错误结果，
使用Arc包装减小复制的开销
6. 使用 block cache 能否保证内存中最多有固定数量的 blocks？
由于Block被Arc包裹，存在引用计数，导致就算有时候超过了也不会被释放

7. 是否可能在LSM引擎中存储列式数据？当前的SST格式仍然是好选择吗？
目前的设计比较适用于行式存储，对于列存储不是很适用
因为这样一次就要存大量的key，

行存储：key：userid | value：name，phone，address 一个人的信息就是一行
列存储：
userid1 | userid2 | userid3
name1   | name2   | name3
age1    | age2    | age3
这样做的好处就是如果我想计算年龄平均值，我只需要扫描一行，就可以计算出来
而行式存储就需要扫描多行，扫描了大量的没有必要的信息

如果在当前的LSM里面对列数据存储，就是一次存储很大的key，
因为我一次存一行，一行的数据都是同一类型

8. 考虑LSM引擎构建在对象存储服务上的情况，你会如何优化SST格式/参数和块缓存？
比如增大缓存，增加索引层，或者优化数据结构进行压缩存储，减少数据传输量

9. 假设为索引预留了16GB内存，你能估算LSM系统能支持的数据库最大大小吗？
索引只需要存储每个block的meta数据就行。
因此 16GB/ meta.size ,再乘以每个块的大小
