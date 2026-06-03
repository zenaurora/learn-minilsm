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

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    key::KeyBytes,
    lsm_storage::LsmStorageState,
    table::SsTable,
};

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

struct OverlappingReturn {
    lower_sst_ids: Vec<usize>,
    first_key: KeyBytes,
    last_key: KeyBytes,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize], // older
        in_level: usize,   // the real index: l1 is 1
    ) -> Option<OverlappingReturn> {
        if sst_ids.is_empty() {
            return None;
        }

        let upper_ssts = sst_ids
            .iter()
            .map(|id| snapshot.sstables.get(id).unwrap())
            .cloned()
            .collect::<Vec<Arc<SsTable>>>();

        if upper_ssts.is_empty() {
            return None;
        }

        let upper_first = upper_ssts.iter().map(|s| s.first_key()).min().unwrap();
        let upper_last = upper_ssts.iter().map(|s| s.last_key()).max().unwrap();

        let mut range_first_key = KeyBytes::from_bytes(Bytes::new());
        let mut range_last_key = KeyBytes::from_bytes(Bytes::new());
        let lower_sst_ids: Vec<usize> = snapshot.levels[in_level - 1]
            .1
            .iter()
            .filter(|&id| {
                let sst = snapshot.sstables.get(id).unwrap();
                let sst_last_key = sst.last_key();
                let sst_first_key = sst.first_key();
                if sst_last_key >= upper_first && sst_first_key <= upper_last {
                    if range_first_key.is_empty() || sst_first_key <= &range_first_key {
                        range_first_key = sst_first_key.clone();
                    }
                    if range_last_key.is_empty() || sst_last_key >= &range_last_key {
                        range_last_key = sst_last_key.clone();
                    }
                    true
                } else {
                    false
                }
            })
            .copied()
            .collect();

        Some(OverlappingReturn {
            lower_sst_ids,
            first_key: range_first_key,
            last_key: range_last_key,
        })
    }

    fn target_sizes_vec(&self, bottom_level_size_mb: usize) -> Vec<usize> {
        let mut target = vec![0_usize; self.options.max_levels];
        // fix: if bottom_level_size too small, the lowest target is always small
        // the lowest level target is at least base_level_size_mb
        let mut cur = bottom_level_size_mb.max(self.options.base_level_size_mb * MB);
        let mut below_base_size = false;
        for i in (0..self.options.max_levels).rev() {
            if cur <= self.options.base_level_size_mb * MB {
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
        // now in level_sizes l1's size is at level_sizes[0]

        let bottom_level_size_byte = level_sizes.last().unwrap();
        let target_sizes = self.target_sizes_vec(*bottom_level_size_byte);
        // now in target_sizes_mb l1's target size is at [0]

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let first_level_not_zero: usize = target_sizes
                .iter()
                .position(|&x| x > 0)
                .unwrap_or(target_sizes.len() - 1);

            let lower_level_sst_ids = if let Some(OverlappingReturn { lower_sst_ids, .. }) = self
                .find_overlapping_ssts(snapshot, &snapshot.l0_sstables, first_level_not_zero + 1)
            {
                lower_sst_ids
            } else {
                vec![]
            };

            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: first_level_not_zero + 1,
                lower_level_sst_ids,
                is_lower_level_bottom_level: first_level_not_zero + 1 == self.options.max_levels,
            });
        }

        let priority_level = level_sizes
            .iter()
            .copied()
            .enumerate()
            .filter(
                // l1 -> i = 0
                |(i, size)| {
                    let target = target_sizes[*i];
                    target > 0 && *size > target
                },
            )
            .max_by(|(i1, s1), (i2, s2)| {
                let ratio1 = *s1 as f64 / (target_sizes[*i1]) as f64;
                let ratio2 = *s2 as f64 / (target_sizes[*i2]) as f64;
                ratio1.total_cmp(&ratio2)
            })
            .map(|(i, _size)| i);

        if let Some(level_index) = priority_level {
            // if level_index + 1 == level_sizes.len() {
            //     // it is the last level, no need to compact
            //     return None;
            // }

            eprintln!(
                "the priority_level is {level_index} from 0 ; levelsizes:{}",
                level_sizes.len()
            );
            eprintln!("target-sizes{target_sizes:?}");
            let upper_level_num = level_index + 1;
            let lower_level_num = upper_level_num + 1;

            let upper_oldest_sst_id = snapshot.levels[level_index].1.iter().min().unwrap();
            // let oldest_sst_first_key = snapshot.sstables[upper_oldest_sst_id].first_key();
            // let oldest_sst_last_key = snapshot.sstables[upper_oldest_sst_id].last_key();

            let mut upper_sst_ids = vec![*upper_oldest_sst_id];
            let mut lower_level_sst_ids = Vec::new();

            // loop to update the upper level chosen ids
            loop {
                let overlapping_return =
                    self.find_overlapping_ssts(snapshot, &upper_sst_ids, lower_level_num);

                if let Some(OverlappingReturn {
                    lower_sst_ids,
                    ref first_key,
                    ref last_key,
                }) = overlapping_return
                {
                    // If lower level has no overlapping SSTs, nothing to align range with
                    if lower_sst_ids.is_empty() {
                        break;
                    }

                    let new_upper_ids: Vec<usize> = snapshot.levels[level_index]
                        .1
                        .iter()
                        .filter(|id| {
                            let sst = snapshot.sstables.get(id).unwrap();
                            sst.first_key() <= last_key && sst.last_key() >= first_key
                        })
                        .copied()
                        .collect();

                    lower_level_sst_ids = lower_sst_ids;
                    if new_upper_ids.len() == upper_sst_ids.len() {
                        // nothing new
                        break;
                    }
                    upper_sst_ids = new_upper_ids;
                } else {
                    break;
                }
            }

            if lower_level_num <= self.options.max_levels {
                return Some(LeveledCompactionTask {
                    upper_level: Some(upper_level_num),
                    // upper_level_sst_ids: snapshot.levels[level_index].1.clone(),
                    upper_level_sst_ids: upper_sst_ids,
                    lower_level: lower_level_num,
                    // lower_level_sst_ids: snapshot.levels[level_index + 1].1.clone(),
                    lower_level_sst_ids,
                    is_lower_level_bottom_level: lower_level_num == self.options.max_levels,
                });
            }
        }

        None
    }

    // 返回新的状态和需要删掉的sst_id
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
