对于manifest record 遍历的时候需要：
如果是newmemtable 需要记录下来这个id，如果是flush，就需要删掉这个id。

接下来需要将所有newmemtable 但是没有flush的memtable 基于id来恢复出来，
然后把这些恢复出来的memtable 加入到immemtable里面

最后再基于max id 生成一个新的memtable