// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::HashMap;
use std::mem;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::{Ok, Result};
use bytes::Bytes;
use crossbeam_channel::Iter;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{MemTable, MemTableIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(())?;
        self.compaction_notifier.send(())?;
        self.flush_thread.lock().take().map(|handle| handle.join());
        self.compaction_thread.lock().take().map(|handle| handle.join());   
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub fn show_memtable_datas(&self) {
        let state = self.state.read();
        println!("Current Memtable Data:");
        for entry in state.memtable.map.iter() {
            println!(
                "Key: {}, Value: {}",
                String::from_utf8_lossy(entry.key()),
                String::from_utf8_lossy(entry.value())
            );
        }

        for (i, imm_memtable) in state.imm_memtables.iter().enumerate() {
            println!("Immutable Memtable {} Data:", i);
            for entry in imm_memtable.map.iter() {
                println!(
                    "Key: {}, Value: {}",
                    String::from_utf8_lossy(entry.key()),
                    String::from_utf8_lossy(entry.value())
                );
            }
        }
    }

    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let state = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        if let Some(value) = state.memtable.get(key) {
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        for imm_memtable in &state.imm_memtables {
            if let Some(value) = imm_memtable.get(key) {
                if value.is_empty() {
                    // Empty value means the key was deleted
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        let mut sstable_iters = Vec::new();

        // L0 SSTs（从新到旧）
        for &sst_id in &state.l0_sstables {
            let sstable = state.sstables.get(&sst_id).unwrap();
            let iter = SsTableIterator::create_and_seek_to_key(
                sstable.clone(),
                KeySlice::from_slice(key),
            )?;

            // 检查是否找到了精确匹配的 key
            if iter.is_valid() && iter.key().raw_ref() == key {
                sstable_iters.push(Box::new(iter));
            }
        }

        // 其他 level 的 SSTs
        for (_level, sst_ids) in &state.levels {
            for &sst_id in sst_ids {
                let sstable = state.sstables.get(&sst_id).unwrap();
                let iter = SsTableIterator::create_and_seek_to_key(
                    sstable.clone(),
                    KeySlice::from_slice(key),
                )?;

                // 检查是否找到了精确匹配的 key
                if iter.is_valid() && iter.key().raw_ref() == key {
                    sstable_iters.push(Box::new(iter));
                }
            }
        }

        let mut sstable_merged_iter = MergeIterator::create(sstable_iters);

        while sstable_merged_iter.is_valid() {
            if sstable_merged_iter.key().raw_ref() == key {
                let value = sstable_merged_iter.value();
                if value.is_empty() {
                    return Ok(None);
                } else {
                    return Ok(Some(Bytes::copy_from_slice(value)));
                }
            } else {
                sstable_merged_iter.next()?;
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let memtable_size = {
            let state = self.state.read();
            state.memtable.put(key, value)?;
            state.memtable.approximate_size()
        }; // Release read lock here

        // Check if we need to freeze memtable (double-check pattern)
        if memtable_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock(); // 状态修改需要使用 Mutex 进行同步
            let state = self.state.read(); // 重新获取读锁
            if state.memtable.approximate_size() >= self.options.target_sst_size {
                drop(state); // Release read lock before calling freeze
                self.force_freeze_memtable(&state_lock)?;
            }
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // println!("Freezing memtable...");
        // state 包含新的 memtable 和 一个 新的 imm_memtable(clone 之前的然后加入新的)
        let new_memtable = if self.options.enable_wal {
            MemTable::create_with_wal(self.next_sst_id(), &self.path)?
        } else {
            MemTable::create(self.next_sst_id())
        };
        {
            let mut state = self.state.write();
            let cur_state = state.as_ref();

            let mut new_imm_table = cur_state.imm_memtables.clone();
            new_imm_table.insert(0, cur_state.memtable.clone());

            // 将外部创建的 new_memtable 放入新的 state
            // 以及 新的 imm_memtable
            let new_state = Arc::new(LsmStorageState {
                memtable: Arc::new(new_memtable),
                imm_memtables: new_imm_table,
                l0_sstables: cur_state.l0_sstables.clone(),
                levels: cur_state.levels.clone(),
                sstables: cur_state.sstables.clone(),
            });

            *state = new_state;
            // println!("Memtable frozen.");
            // println!(
            //     "new state immutable memtables: {}",
            //     state.imm_memtables.len()
            // );
        }
        Ok(())
        // unimplemented!()
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {

        let memtable_to_flush;

        // create new sstable using the imm_memtable.last()
        let sstable= {
            let guard = self.state.read();

            if let Some(memtable) = guard.imm_memtables.last() {
                memtable_to_flush = Arc::clone(memtable);
            } else {
                return Ok(());
            }
            let sst_path = self.path_of_sst(memtable_to_flush.id());

            let mut sstable_builder = SsTableBuilder::new(self.options.block_size);

            memtable_to_flush.flush(&mut sstable_builder)?;

            let sstable =
                sstable_builder.build(memtable_to_flush.id(), Some(self.block_cache.clone()), sst_path)?;

            sstable
        };

        // heavy write operation, use state_lock to synchronize
        {   
            let _state_lcok = self.state_lock.lock();

            let mut state = self.state.write();
            let cur_state = state.as_ref();

            let mut new_imm_tables = cur_state.imm_memtables.clone();
            new_imm_tables.pop();

            let mut new_l0_sstables = cur_state.l0_sstables.clone();
            new_l0_sstables.insert(0, memtable_to_flush.id());

            let new_state = Arc::new(LsmStorageState {
                memtable: cur_state.memtable.clone(),
                imm_memtables: new_imm_tables,
                l0_sstables: new_l0_sstables,
                levels: cur_state.levels.clone(),
                sstables: {
                    let mut new_sstables = cur_state.sstables.clone();
                    new_sstables.insert(memtable_to_flush.id(), Arc::new(sstable));
                    new_sstables
                },
            });

            *state = new_state;
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    fn get_twomerged_iter(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>>
    {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // 收集所有 memtable 的迭代器
        let mut mem_iters = Vec::new();

        // 当前 memtable
        mem_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));

        // 所有不可变 memtables（从新到旧）
        // 从新到旧的原因是因为每次新插入的都是从开头插入的
        for imm_memtable in &snapshot.imm_memtables {
            mem_iters.push(Box::new(imm_memtable.scan(lower, upper)));
        }

        // create SSTable iterators
        let mut sst_iters = Vec::new();
        for &sst_id in &snapshot.l0_sstables {
            let sstable: &Arc<SsTable> = snapshot.sstables.get(&sst_id).unwrap();

            let first_key = sstable.first_key();
            let last_key = sstable.last_key();

            match upper {
                Bound::Included(upper_key) => {
                    if first_key.raw_ref() > upper_key {
                        continue;
                    }
                }
                Bound::Excluded(upper_key) => {
                    if first_key.raw_ref() >= upper_key {
                        continue;
                    }
                }
                Bound::Unbounded => {}
            }

            match lower {
                Bound::Included(lower_key) => {
                    if last_key.raw_ref() < lower_key {
                        continue;
                    }

                    let iter = SsTableIterator::create_and_seek_to_key(
                        sstable.clone(),
                        KeySlice::from_slice(lower_key),
                    )?;

                    if iter.is_valid() && iter.key().raw_ref() == lower_key {
                    } else {
                        continue;
                    }

                    sst_iters.push(Box::new(iter));
                }
                Bound::Excluded(lower_key) => {
                    if last_key.raw_ref() <= lower_key {
                        continue;
                    }
                    let mut iter = SsTableIterator::create_and_seek_to_key(
                        sstable.clone(),
                        KeySlice::from_slice(lower_key),
                    )?;

                    iter.next()?;

                    if iter.is_valid() && iter.key().raw_ref() > lower_key {
                    } else {
                        continue;
                    }
                    sst_iters.push(Box::new(iter));
                }
                Bound::Unbounded => {
                    let iter = SsTableIterator::create_and_seek_to_first(sstable.clone())?;
                    if !iter.is_valid() {
                        continue;
                    }
                    sst_iters.push(Box::new(iter));
                }
            }
        }

        for (_level, sst_ids) in &snapshot.levels {
            for &sst_id in sst_ids {
                let sstable: &Arc<SsTable> = snapshot.sstables.get(&sst_id).unwrap();

                let first_key = sstable.first_key();
                let last_key = sstable.last_key();

                match upper {
                    Bound::Included(upper_key) => {
                        if first_key.raw_ref() > upper_key {
                            continue;
                        }
                    }
                    Bound::Excluded(upper_key) => {
                        if first_key.raw_ref() >= upper_key {
                            continue;
                        }
                    }
                    Bound::Unbounded => {}
                }

                match lower {
                    Bound::Included(lower_key) => {
                        if last_key.raw_ref() < lower_key {
                            continue;
                        }

                        let iter = SsTableIterator::create_and_seek_to_key(
                            sstable.clone(),
                            KeySlice::from_slice(lower_key),
                        )?;

                        if iter.is_valid() && iter.key().raw_ref() == lower_key {
                        } else {
                            continue;
                        }
                        sst_iters.push(Box::new(iter));
                    }
                    Bound::Excluded(lower_key) => {
                        if last_key.raw_ref() <= lower_key {
                            continue;
                        }
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            sstable.clone(),
                            KeySlice::from_slice(lower_key),
                        )?;

                        iter.next()?;

                        if iter.is_valid() && iter.key().raw_ref() > lower_key {
                        } else {
                            continue;
                        }
                        sst_iters.push(Box::new(iter));
                    }
                    Bound::Unbounded => {
                        let iter = SsTableIterator::create_and_seek_to_first(sstable.clone())?;
                        if !iter.is_valid() {
                            continue;
                        }
                        sst_iters.push(Box::new(iter));
                    }
                }
            }
        }

        // 创建 merge iterator
        let merge_memtable_iter = MergeIterator::create(mem_iters);
        let merge_sstable_iter = MergeIterator::create(sst_iters);

        let new_iter = TwoMergeIterator::create(merge_memtable_iter, merge_sstable_iter)?;
        Ok(new_iter)
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let new_iter = self.get_twomerged_iter(lower, upper)?;

        // 创建end_bound 字段
        let end_bound = match upper {
            Bound::Included(u) => Bound::Included(Bytes::from(u.to_vec())),
            Bound::Excluded(u) => Bound::Excluded(Bytes::from(u.to_vec())),
            Bound::Unbounded => Bound::Unbounded,
        };
        // println!("MergeIterator created.");
        // 创建 LsmIterator，inner 是 TwoMergeIterator
        let mut lsm_iter = LsmIterator::new(new_iter, end_bound)?;
        // println!("LsmIterator created.");

        // 判断一个key是否被删除，需要跳过这些
        // 教训：注意要把跳过删除的key的逻辑放在这里，
        // 而非其他的底层结构：MergerIter，MemTableIter...
        while lsm_iter.is_valid() && lsm_iter.value().is_empty() {
            lsm_iter.next()?;
        }

        // FusedIterator 保证了迭代器一旦失效就什么都不做(返回Ok(()))
        // 而不是报错, FusedIterator 只是一层简单的包装
        let fused_iter = FusedIterator::new(lsm_iter);

        Ok(fused_iter)
    }
}
