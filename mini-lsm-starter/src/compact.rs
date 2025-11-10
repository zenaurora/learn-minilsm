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

use std::os::linux::raw::stat;
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
use crate::iterators::merge_iterator::MergeIterator;
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
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::Leveled(_leveled_task) => {
                // TODO(you): implement leveled compaction
                unimplemented!()
            }
            CompactionTask::Simple(_simple_task) => {
                // TODO(you): implement simple leveled compaction
                unimplemented!()
            }
            CompactionTask::Tiered(_tiered_task) => {
                // TODO(you): implement tiered compaction
                unimplemented!()
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let sstables = {
                    let state = self.state.read();
                    let mut ssts = Vec::new();
                    for sst_id in l0_sstables.iter().chain(l1_sstables.iter()) {
                        if let Some(sst) = state.sstables.get(sst_id) {
                            ssts.push(sst.clone());
                        }
                    }
                    ssts
                };

                if sstables.is_empty() {
                    return Ok(Vec::new());
                }

                let iters = sstables
                    .into_iter()
                    .map(|sst| Box::new(SsTableIterator::create_and_seek_to_first(sst).unwrap()))
                    .collect::<Vec<_>>();

                let mut merged_iter = MergeIterator::create(iters);

                let mut builder = SsTableBuilder::new(self.options.block_size);

                let mut new_sstables: Vec<Arc<SsTable>> = Vec::new();
                let mut id = self.next_sst_id();
                while merged_iter.is_valid() {
                    let key = merged_iter.key();
                    let value = merged_iter.value();

                    if task.compact_to_bottom_level() && value.is_empty() {
                        // this is deleted
                        merged_iter.next()?;
                        continue;
                    }

                    builder.add(key, value);
                    if builder.estimated_size() > self.options.target_sst_size {
                        let sstable = builder.build(
                            id,
                            Some(self.block_cache.clone()),
                            self.path_of_sst(id),
                        )?;
                        new_sstables.push(Arc::new(sstable));

                        id = self.next_sst_id();

                        builder = SsTableBuilder::new(self.options.block_size);
                    }
                    merged_iter.next()?;
                }

                // add the last sstable if there are remaining entries
                if builder.estimated_size() > 0 {
                    new_sstables.push(Arc::new(builder.build(
                        id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(id),
                    )?));
                }

                Ok(new_sstables)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let ssts_to_compact = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: ssts_to_compact.0.clone(),
            l1_sstables: ssts_to_compact.1.clone(),
        };

        let compacted_sstables = self.compact(&task)?;

        let ids_to_remove = &ssts_to_compact
            .0
            .iter()
            .chain(ssts_to_compact.1.iter())
            .cloned()
            .collect::<Vec<_>>();
        {
            let lock = self.state_lock.lock();
            let mut lsm_state = self.state.write();

            let (new_state, obsolete_sst_ids) = self.compaction_controller.apply_compaction_result(
                &lsm_state,
                &task,
                &compacted_sstables
                    .iter()
                    .map(|sst| sst.sst_id())
                    .collect::<Vec<_>>(),
                false,
            );
            *lsm_state = Arc::new(new_state);
        }

        // remove old sst
        for sst_id in ids_to_remove {
            std::fs::remove_file(self.path_of_sst(*sst_id))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
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
