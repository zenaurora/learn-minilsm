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

mod leveled;
mod simple_leveled;
mod tiered;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn generate_new_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = SsTableBuilder::new(self.options.block_size);

        let mut new_sstables: Vec<Arc<SsTable>> = Vec::new();
        let mut id = self.next_sst_id();
        let mut has_new_builder = false;
        while iter.is_valid() {
            has_new_builder = true;
            let key = iter.key();
            let value = iter.value();
            if compact_to_bottom_level && value.is_empty() {
                iter.next()?;
                continue;
            }

            builder.add(key, value);
            if builder.estimated_size() > self.options.target_sst_size {
                let sstable =
                    builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;
                new_sstables.push(Arc::new(sstable));
                has_new_builder = false;
                id = self.next_sst_id();
                builder = SsTableBuilder::new(self.options.block_size);
            }
            iter.next()?;
        }

        // add the last sstable if there are remaining entries
        // if builder.estimated_size() > 0 {
        // 这句话不能加，因为如果add的时候没有触发超过一个blcok size
        // 那么data里面就不会有数据
        if has_new_builder {
            new_sstables.push(Arc::new(builder.build(
                id,
                Some(self.block_cache.clone()),
                self.path_of_sst(id),
            )?));
        }
        Ok(new_sstables)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        match task {
            // CompactionTask::Leveled(LeveledCompactionTask{
            //     is_lower_level_bottom_level,
            //     lower_level,
            //     lower_level_sst_ids,
            //     upper_level,
            //     upper_level_sst_ids,
            // }) => {
            //     // TODO(you): implement leveled compaction

            //     unimplemented!()
            // }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            }) => {
                // need to check if upper level is empty
                // 由于每次flush直接写到l0 所以l0 需要去重
                // 但是l1+ 就不会出现key范围重叠的情况
                match upper_level {
                    // L1+, just use ConcatIterator simply
                    Some(_) => {
                        let upper_ssts = upper_level_sst_ids
                            .iter()
                            .map(|id| snapshot.sstables.get(id).unwrap())
                            .cloned()
                            .collect::<Vec<_>>();

                        let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;

                        let lower_ssts = lower_level_sst_ids
                            .iter()
                            .map(|id| snapshot.sstables.get(id).unwrap())
                            .cloned()
                            .collect::<Vec<_>>();
                        let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;

                        self.generate_new_sst_from_iter(
                            TwoMergeIterator::create(upper_iter, lower_iter)?,
                            task.compact_to_bottom_level(),
                        )
                    }
                    // L0, need to use MergeIterator in upper iter
                    None => {
                        // let mut upper_iters = Vec::with_capacity(upper_level_sst_ids.len()) ;
                        let upper_iters = upper_level_sst_ids
                            .iter()
                            .map(|id| {
                                SsTableIterator::create_and_seek_to_first(
                                    snapshot.sstables.get(id).unwrap().clone(),
                                )
                                .map(Box::new)
                            })
                            .collect::<Result<Vec<_>>>()?;

                        let upper_iter = MergeIterator::create(upper_iters);

                        let lower_ssts = lower_level_sst_ids
                            .iter()
                            .map(|id| snapshot.sstables.get(id).unwrap())
                            .cloned()
                            .collect::<Vec<_>>();
                        let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                        self.generate_new_sst_from_iter(
                            TwoMergeIterator::create(upper_iter, lower_iter)?,
                            task.compact_to_bottom_level(),
                        )
                    }
                }
            }
            CompactionTask::Tiered(TieredCompactionTask {
                tiers,
                bottom_tier_included,
            }) => {
                let sst_iters = tiers
                    .iter()
                    .map(|(_, ssts)| {
                        ssts.iter().map(|id| {
                            SsTableIterator::create_and_seek_to_first(
                                snapshot.sstables.get(id).unwrap().clone(),
                            )
                            .map(Box::new)
                        })
                        // .flatten()
                    })
                    .flatten()
                    .collect::<Result<Vec<_>>>()?;
                let merged_iter = MergeIterator::create(sst_iters);
                self.generate_new_sst_from_iter(merged_iter, task.compact_to_bottom_level())
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                // 获取l0和l1的所有sstbale,然后使用合并迭代器合并
                // 然后重新生成一个新的sstbale，返回给force_full_compaction调用处

                // FIX: l1 should use ConcatIterator

                let (l0ssts, l1_ssts) = {
                    let state = self.state.read();
                    let l0_ssts = l0_sstables
                        .iter()
                        .filter_map(|id| state.sstables.get(id))
                        .cloned()
                        .collect::<Vec<_>>();
                    let mut l1_ssts = l1_sstables
                        .iter()
                        .filter_map(|id| state.sstables.get(id))
                        .cloned()
                        .collect::<Vec<_>>();
                    l1_ssts.sort_by(|a, b| a.first_key().cmp(b.first_key()));
                    (l0_ssts, l1_ssts)
                };

                if l0ssts.is_empty() && l1_ssts.is_empty() {
                    return Ok(Vec::new());
                }

                let l0_iters = l0ssts
                    .into_iter()
                    .map(|sst| Box::new(SsTableIterator::create_and_seek_to_first(sst).unwrap()))
                    .collect::<Vec<_>>();
                let l0_merged_iter = MergeIterator::create(l0_iters);

                let l1_concat_iter = SstConcatIterator::create_and_seek_to_first(l1_ssts)?;

                // println!("=====Created {} iterators for merging", iters.len());
                let merged_iter = TwoMergeIterator::create(l0_merged_iter, l1_concat_iter)?;

                // self.generate_new_sst_from_iter(merged_iter, task.compact_to_bottom_level())
                // 返回的是新的sstables
                self.generate_new_sst_from_iter(merged_iter, task.compact_to_bottom_level())
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_ids, l1_ids) = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_ids.clone(),
            l1_sstables: l1_ids.clone(),
        };

        let compacted_sstables = self.compact(&task)?;

        // 获取要删除的old sst id
        let ids_to_remove = &l0_ids
            .iter()
            .chain(l1_ids.iter())
            .cloned()
            .collect::<Vec<_>>();

        // update lsm state
        {
            let lock = self.state_lock.lock();
            let mut new_state = self.state.read().as_ref().clone();
            new_state.l0_sstables.clear();
            let new_l1_ids: Vec<usize> =
                compacted_sstables.iter().map(|sst| sst.sst_id()).collect();

            new_state.levels[0].1 = new_l1_ids;

            // remove old sst from state
            for id in ids_to_remove {
                new_state.sstables.remove(id);
            }

            for sst in compacted_sstables {
                new_state.sstables.insert(sst.sst_id(), Arc::clone(&sst));
            }

            // use write lock here, instead of upgrading read lock to write lock
            *self.state.write() = Arc::new(new_state);
            self.sync_dir()?;
        }

        // remove old sst for OS
        for sst_id in ids_to_remove {
            std::fs::remove_file(self.path_of_sst(*sst_id))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        // 触发compaction任务时候不包括force_full_compaction任务
        // 只有Leveled、Simple、Tiered三种compaction任务会触发compaction
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };

        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);

        if let Some(task) = task {
            // 获取新的sstables
            let compacted_sstables = self.compact(&task)?;

            // 获取新的sstables的id
            let output = compacted_sstables
                .iter()
                .map(|sst| sst.sst_id())
                .collect::<Vec<_>>();

            // 应用compaction结果（基于snapshot计算应该如何修改）
            let (new_state_from_snapshot, obsolete_ssts) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);

            // update lsm state
            {
                let _lock = self.state_lock.lock();
                // 读取当前最新状态，避免丢掉并发 flush 的 L0
                let mut new_state = self.state.read().as_ref().clone();

                // 保留当前状态中的 L0（去掉本次 compaction 废弃的文件）
                new_state
                    .l0_sstables
                    .retain(|id| !obsolete_ssts.contains(id));

                // 覆盖 levels（只 compaction 会改 levels，flush 不会）
                new_state.levels = new_state_from_snapshot.levels;
                // 因为可能l0有新flush下来的，所以不能应用snapshot里面的值，只需要保留就行
                // new_state.l0_sstables = new_state_from_snapshot.l0_sstables; -- WRONG
                // 添加新生成的 SST
                for sst in compacted_sstables {
                    new_state.sstables.insert(sst.sst_id(), sst);
                }

                // 移除废弃的 SST
                for id in &obsolete_ssts {
                    new_state.sstables.remove(id);
                }

                *self.state.write() = Arc::new(new_state);
                self.sync_dir()?;
            }
            // remove obsolete sstables for OS
            for id in obsolete_ssts {
                std::fs::remove_file(self.path_of_sst(id))?;
            }
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        // let state = self.state.read();
        // println!(
        //     "trying to trigger flush, imm_memtables len: {},limit = {}",
        //     state.imm_memtables.len(),self.options.num_memtable_limit
        // );
        // if state.imm_memtables.len() + 1 > self.options.num_memtable_limit {
        //     self.force_flush_next_imm_memtable()?;
        // }
        // println!("flush triggered");
        // Ok(())
        let should_flush = {
            let state = self.state.read();
            // println!(
            //     "trying to trigger flush, imm_memtables len: {}, limit = {}",
            //     state.imm_memtables.len(),
            //     self.options.num_memtable_limit
            // );
            state.imm_memtables.len() >= self.options.num_memtable_limit
        }; // ← read lock 在这里释放

        if should_flush {
            self.force_flush_next_imm_memtable()?;
        }

        // println!("flush triggered");
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                // 每50ms执行一次flush
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
