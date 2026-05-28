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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize, // tiers超过此数时发生压缩
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        // 只有超过num_tiers 时候才触发压缩任务
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        let level_len = snapshot.levels.len();
        let nums_tiers = self.options.num_tiers;

        // 1.1 amp ratio
        let mut except_last_size = 0;
        for i in 0..snapshot.levels.len() - 1 {
            except_last_size += snapshot.levels[i].1.len();
        }
        let last_level_size = snapshot.levels.last().unwrap().1.len();
        let amp_ratio = (except_last_size as f64) / (last_level_size as f64) * 100.0;
        if amp_ratio >= self.options.max_size_amplification_percent as f64 {
            println!("generate_compaction_tiered_task");
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        /*
            Tier 3: 1
            Tier 2: 1 ; 1 / 1 = 1
            Tier 1: 3 ; 3 / (1 + 1) = 1.5, compact tier 2+3
            1.2 size ratio
        */
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut previous_sum = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            previous_sum += snapshot.levels[id].1.len();
            let next_level_size = snapshot.levels[id + 1].1.len();
            let current_size_ratio = next_level_size as f64 / previous_sum as f64;
            if current_size_ratio > size_ratio_trigger && id + 1 >= self.options.min_merge_width {
                return Some(TieredCompactionTask {
                    tiers: snapshot
                        .levels
                        .iter()
                        .take(id + 1)
                        .cloned()
                        .collect::<Vec<_>>(),
                    bottom_tier_included: false,
                });
            }
        }

        // 1.3 reduce sorted runs
        // I dont know why the official implement use usize::MAX
        let max_tiers = self.options.max_merge_width.unwrap_or(usize::MAX);
        let take_count = max_tiers.min(snapshot.levels.len());
        Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(take_count)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: take_count >= level_len,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = snapshot.clone();

        let tiers_and_ssts_map = task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();
        // 所有需要删掉的sst_id, 来自于每个需要删掉的tier里面的全部的sst_id
        let mut ssts_id_to_remove: Vec<usize> = Vec::new();

        // 所有需要删掉的tier
        let levels_compacted = &task.tiers;

        new_state.levels.retain(|(tier_id, sst_ids)| {
            if tiers_and_ssts_map.contains_key(tier_id) {
                assert_eq!(&sst_ids, tiers_and_ssts_map.get(tier_id).unwrap());
                ssts_id_to_remove.extend(sst_ids.iter().copied());
                false
            } else {
                true
            }
        });

        new_state.levels.insert(0, (output[0], output.to_vec()));
        println!("apply compaction result successful");
        (new_state, ssts_id_to_remove)
    }
}
