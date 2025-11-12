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

use std::{sync::Arc, thread::current};

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>, // already sorted by their key ranges
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let mut iter = SstConcatIterator {
            current: None,
            next_sst_idx: 0,
            sstables,
        };

        if !iter.sstables.is_empty() {
            let first_sst = iter.sstables[0].clone();
            let ss_table_iter = SsTableIterator::create_and_seek_to_first(first_sst)?;
            iter.current = Some(ss_table_iter);
            iter.next_sst_idx = 1;
        }

        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut iter = SstConcatIterator {
            current: None,
            next_sst_idx: 0,
            sstables,
        };

        if iter.sstables.is_empty() {
            return Ok(iter);
        }

        let idx = iter
            .sstables
            .partition_point(|s| s.last_key().as_key_slice() < key);

        if idx < iter.sstables.len() {
            let sst = iter.sstables[idx].clone();
            let ss_table_iter = SsTableIterator::create_and_seek_to_key(sst, key)?;
            if ss_table_iter.is_valid() {
                iter.current = Some(ss_table_iter);
                iter.next_sst_idx = idx + 1;
            } else {
                // 由于range 不重合，找不到
                iter.current = None;
            }
        }

        Ok(iter)
    }

    fn move_to_next_sst(&mut self) -> Result<()> {
        if self.next_sst_idx < self.sstables.len() {
            let next_sst = self.sstables[self.next_sst_idx].clone();
            let ss_table_iter = SsTableIterator::create_and_seek_to_first(next_sst)?;
            self.current = Some(ss_table_iter);
            self.next_sst_idx += 1;
        } else {
            self.current = None;
        }
        Ok(())
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&'_ self) -> KeySlice<'_> {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if let Some(cur) = &self.current {
            cur.is_valid()
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        if let Some(cur) = &mut self.current {
            cur.next()?;
            if cur.is_valid() {
                return Ok(());
            } else {
                // move to next sst
                self.move_to_next_sst()?;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.current.as_ref().map_or(0, |_| 1)
    }
}
