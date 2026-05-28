1. 在进行压缩操作时，使用/填充块缓存是否明智？还是在压缩操作时完全绕过块缓存更好？
不应该去填充，因为压缩的块很多都不是需要使用的热点数据，不应该占用宝贵的cache空间
按照ai的说法，可以把一些元数据之类的放在一个专用的cache里面应该是一个可以的方案

---

## Tiered Compaction - Test Your Understanding

### Q1: Tiered Compaction 的写放大估算（没有最后的 reduce sorted runs 触发条件）

**有所有触发条件时**（simulator 默认）：写放大约 ~3.7x

**没有 "reduce sorted runs" 触发条件时**：
- 只有 space amplification trigger 和 size ratio trigger 工作
- Space amp trigger 是低频但高开销的（合并所有 tier）
- Size ratio trigger 频繁触发，每次只合并最新的几个小 tier
- 没有最后那个 fallback trigger，数据会慢慢堆积更多 tier，write amplification 反而**略低**（因为少发生了一些合并），但 read amplification 会升高
- 估算：在 size ratio = 10% 时，每个数据在到达底层前平均被合并 log_{1.1}(N) 次，写放大 ≈ **10-15x**

---

### Q2: Tiered Compaction 的读放大估算

读放大 = tier 的数量（最坏情况下每个 tier 都要检查）

- `num_tiers = 7` → 读放大 ≈ **7x**
- 即使有 bloom filter，对每个 tier 都需要做一次 bloom filter 查询
- 与 Leveled Compaction 相比（读放大 ≈ L 层数，通常 5-7x），差不多，但 tiered 的 tier 内还需要二分查找

---

### Q3: Universal Compaction vs Simple Leveled/Tiered 的优缺点

| 维度 | Universal (Tiered) | Simple Leveled |
|------|-------------------|----------------|
| 写放大 | ✅ 低（~10x） | ❌ 高（~30-40x） |
| 读放大 | ❌ 高（= tier 数） | ✅ 低（= level 数，通常 5-7x） |
| 空间放大 | ❌ 高（compaction 期间需要 2x 空间） | ✅ 低（约 1.1x） |
| 适合场景 | 写密集型 workload | 读密集型 workload |
| 实现复杂度 | 中等 | 较简单 |

**结论**：Universal Compaction 是写性能和读性能之间的权衡选择，适合写多读少的场景（如日志、事件流）。

---

### Q4: Universal Compaction 需要多少存储空间？

在 compaction 执行期间（合并所有 tier 时）：
- 需要同时保留**旧的所有 tier** + **新合并的输出**
- 最坏情况 = **2x 用户数据大小**（即 100% 空间放大）

simulator 的实际测量约为 1.4x~1.88x，取决于参数配置（`max_size_amplification_percent`）。

---

### Q5: 能否合并 LSM 状态中不相邻的两个 tier？

**理论上可以**，但**实践中不这样做**，原因是：

1. **语义正确性**：Tiered compaction 中，tier 按新到旧排列，tier[0] 是最新的。合并非相邻的 tier[0] 和 tier[2]，但保留中间的 tier[1]，合并结果应该放在 tier[1] 和 tier[3] 之间——但 tier[1] 可能包含和合并结果重叠的 key，"谁更新"的语义会混乱。
2. **实现复杂**：需要处理中间 tier 的位置关系，容易出 bug。
3. **没有收益**：相邻合并已经能满足所有触发条件的需求。

---

### Q6: 如果压缩速度跟不上 SST flush 速度会发生什么？

1. **Tier 不断堆积** → 读放大线性增长
2. **Tombstone 无法及时清理** → 磁盘空间持续增长
3. **最终磁盘写满**或内存耗尽
4. 系统应该触发 **write stall（写限速）** 甚至 **write stop** 来给压缩让路
   - RocksDB 对应配置：`level0_slowdown_writes_trigger` / `level0_stop_writes_trigger`
5. 这是生产系统中 LSM 引擎最常见的性能危机场景之一

---

### Q7: 并行调度多个 compaction 任务需要考虑什么？

1. **任务独立性**：确保两个并发任务不压缩同一批 SST（避免重复处理）
2. **原子状态更新**：应用压缩结果时需要原子地更新 LSM state（`state_lock`）
3. **SST ID 不冲突**：并发任务的输出 SST 必须有全局唯一 ID（原子计数器）
4. **文件生命周期管理**：正在被某任务读取的 SST 不能被另一个任务删除（引用计数 / MVCC 版本管理）
5. **死锁预防**：多任务并发读写 state 时需要严格的锁顺序
6. **任务优先级**：高层（更新数据多）的压缩应该优先于低层

---

### Q8: SSD 自身写放大 2x，整体端到端写放大是多少？

**总写放大 = LSM 写放大 × SSD 内部写放大**

- Tiered compaction（~10x LSM WA）：10x × 2 = **20x**
- Leveled compaction（~40x LSM WA）：40x × 2 = **80x**

**ZNS（Zoned Namespace SSD）** 的意义：
- 传统 SSD 有 FTL（Flash Translation Layer），内部自己管理数据放置，导致 2-4x 内部写放大
- ZNS 把 Zone 接口暴露给应用（LSM 引擎），应用自己控制数据写入哪个 Zone
- FTL 的内部 GC 开销几乎消除，SSD 内部写放大降到 ~1x
- 整体写放大减半：Tiered 10x × 1 = **10x**，Leveled 40x × 1 = **40x**

---

### Q9: 300 个 Tier 时，能否用 O(log n) 数据结构加速读取？

**当前做法**：对每个 tier 做二分查找定位目标 SST → O(300 × log M)（M = 每个 tier 内 SST 数量）

**改进方案**：维护一个跨所有 tier 的**按 key range 索引的数据结构**（如 BTree 或区间树）：
- 每个 SST 作为一个 `(first_key, last_key, tier_id, sst_id)` 条目插入
- 查找时：给定目标 key，O(log(300×M)) 找到所有可能包含该 key 的 SST

**Neon 的 Layer Map 实现**：
- 用类似 `BTreeMap<(key_range, LSN)>` 的结构索引所有 Layer（类似 tier）
- 对给定 key 和 LSN，可以在 O(log N) 内找到所有相关 layer，而不是线性扫描 300 个
- 这对超大 tier 数量的 tiered compaction 非常有价值

**是否值得做**：当 tier 数量很大（如 300）时，是的，收益显著。但普通场景（7-16 个 tier）开销不大，不必要。


