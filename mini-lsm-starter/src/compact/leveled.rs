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

use std::sync::Arc;

use parking_lot::OnceState::New;
use serde::{Deserialize, Serialize};

use crate::{lsm_storage::LsmStorageState, table::SsTable};

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

const MB: usize = 1024 * 1024;

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize], // older
        in_level: usize,
    ) -> Vec<usize> {
        if sst_ids.is_empty() {
            return Vec::new();
        }

        let upper_ssts = sst_ids
            .iter()
            .map(|id| snapshot.sstables.get(id).unwrap())
            .cloned()
            .collect::<Vec<Arc<SsTable>>>();

        if upper_ssts.is_empty() {
            return Vec::new();
        }

        let upper_first = upper_ssts.iter().map(|s| s.first_key()).min().unwrap();
        let upper_last = upper_ssts.iter().map(|s| s.last_key()).max().unwrap();
        snapshot.levels[in_level - 1]
            .1
            .iter()
            .filter(|&id| {
                let sst = snapshot.sstables.get(id).unwrap();
                sst.last_key() >= upper_first && sst.first_key() <= upper_last
            })
            .copied()
            .collect()
    }

    fn target_sizes_vec(&self, bottom_level_size_mb: usize) -> Vec<usize> {
        let mut target = vec![0_usize; self.options.max_levels];
        let mut cur = bottom_level_size_mb;
        let mut below_base_size = false;
        for i in (0..self.options.max_levels).rev() {
            if cur < self.options.base_level_size_mb {
                target[i] = if !below_base_size {
                    below_base_size = true;
                    cur
                } else {
                    break;
                }
            } else {
                target[i] = cur;
                cur /= self.options.level_size_multiplier;
            }
        }
        target
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let level_sizes: Vec<usize> = snapshot
            .levels
            .iter()
            .map(|(_level_id, sst_ids)| {
                sst_ids
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().table_size() as usize)
                    .sum()
            })
            .collect();

        let bottom_level_size_mb = level_sizes.last().unwrap() / MB;
        let target_sizes_mb = self.target_sizes_vec(bottom_level_size_mb);

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            // NOTE: target index is from zero to len()
            // l1's index in target_sizes is 0
            let first_level_not_zero: usize = target_sizes_mb
                .iter()
                .position(|&x| x > 0)
                .unwrap_or(target_sizes_mb.len() - 1);

            let lower_level_sst_ids = self.find_overlapping_ssts(
                snapshot,
                &snapshot.l0_sstables,
                first_level_not_zero + 1,
            );

            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: first_level_not_zero + 1,
                lower_level_sst_ids,
                is_lower_level_bottom_level: first_level_not_zero + 1 == self.options.max_levels,
            });
        }

        // snapshot.levels in 1-based\
        let priority_level = level_sizes
            .iter()
            .copied()
            .enumerate()
            .filter(
                // l1 -> i = 0
                |(i, size)| {
                    let target_mb = target_sizes_mb[*i];
                    target_mb > 0 && *size > target_mb * MB
                },
            )
            .max_by(|(i1, s1), (i2, s2)| {
                let ratio1 = *s1 as f64 / (target_sizes_mb[*i1] * MB) as f64;
                let ratio2 = *s2 as f64 / (target_sizes_mb[*i2] * MB) as f64;
                ratio1.partial_cmp(&ratio2).unwrap()
            })
            .map(|(i, _size)| i);
        // let priority_level = snapshot
        //     .levels
        //     .iter()
        //     .enumerate()
        //     .filter(|(i, level)| {
        //         let target = target_sizes[*i];
        //         target > 0 && level.1.len() > target
        //     })
        //     .max_by(|(i1, l1), (i2, l2)| {
        //         let ratio1 = l1.1.len() as f64 / target_sizes[*i1] as f64;
        //         let ratio2 = l2.1.len() as f64 / target_sizes[*i2] as f64;
        //         ratio1.partial_cmp(&ratio2).unwrap()
        //     })
        //     .map(|(i, _)| i); // i is 0-based array index

        if let Some(level_index) = priority_level {
            let upper_level_num = level_index + 1;
            let lower_level_num = upper_level_num + 1;

            let upper_oldest_sst_id = snapshot.levels[level_index].1.iter().min().unwrap().clone();
            let lower_level_sst_ids =
                self.find_overlapping_ssts(snapshot, &[upper_oldest_sst_id], lower_level_num);

            if lower_level_num <= self.options.max_levels {
                return Some(LeveledCompactionTask {
                    upper_level: Some(upper_level_num),
                    // upper_level_sst_ids: snapshot.levels[level_index].1.clone(),
                    upper_level_sst_ids: vec![upper_oldest_sst_id],
                    lower_level: lower_level_num,
                    // lower_level_sst_ids: snapshot.levels[level_index + 1].1.clone(),
                    lower_level_sst_ids,
                    is_lower_level_bottom_level: lower_level_num == self.options.max_levels,
                });
            }
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = snapshot.clone();

        // let mut sst_ids_to_remove: Vec<usize> = Vec::new();

        if let Some(upper_level) = task.upper_level {
            new_state.levels[upper_level - 1]
                .1
                .retain(|x| !task.upper_level_sst_ids.contains(x));
        } else {
            new_state
                .l0_sstables
                .retain(|x| !task.upper_level_sst_ids.contains(x));
        }

        new_state.levels[task.lower_level - 1]
            .1
            .retain(|x| !task.lower_level_sst_ids.contains(x));

        let sst_ids_to_remove: Vec<usize> = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .chain(task.lower_level_sst_ids.iter().copied())
            .collect();

        new_state.levels[task.lower_level - 1]
            .1
            .extend_from_slice(output);

        // in recovery 意思是是否是恢复模式，恢复模式不需要排序，因为可能没有firstkey信息
        if !in_recovery {
            new_state.levels[task.lower_level - 1].1.sort_by(|a, b| {
                snapshot
                    .sstables
                    .get(a)
                    .unwrap()
                    .first_key()
                    .cmp(snapshot.sstables.get(b).unwrap().first_key())
            });
        }

        (new_state, sst_ids_to_remove)
        // unimplemented!()
    }
}
