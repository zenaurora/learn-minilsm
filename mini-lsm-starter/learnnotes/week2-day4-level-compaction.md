Consider the case that the upper level has two tables of [100, 200], [201, 300] and the lower level has [50, 150], [151, 250], [251, 350]. In this case, do you still want to compact one file in the upper level at a time? Why?

这个如果分两次合并的话，会出现一次额外的写入：
[100,200]
overlap: [50,150],[151,250]

after compact : 
upper: [201,300]
lower: [50,250] [251,350]

然后再来一次合并：
[201,300] overlap: [50,250],[251,350]
也就是刚刚新合并出来的[50,250]又重新写了一次
所以在这个情况下应该一次性合并两个而非只选择一个最旧的

改进思路：
选中上层第一个 SST 后，继续检查上层中键范围与其相邻或重叠的SST，
将它们一并纳入本次 compaction 任务，直到某个上层 SST 与当前合并范围不再有交集为止。
这样一次 compaction 覆盖的下层 SST 集合不会变化（还是那几个），
但避免了中间产物被下一次 compaction 重复读写，从而降低写放大。