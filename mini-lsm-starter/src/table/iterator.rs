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

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block_cached(0)?;

        let blk_iter = BlockIterator::create_and_seek_to_first(block);
        let iter = Self {
            table,
            blk_iter,
            blk_idx: 0,
        };

        // iter.seek_to_first()?;
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        let block = self.table.read_block_cached(self.blk_idx)?;
        self.blk_iter = BlockIterator::create_and_seek_to_first(block);

        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut iter = Self {
            table: table.clone(),
            blk_iter: BlockIterator::create_and_seek_to_first(table.read_block_cached(0)?),
            blk_idx: 0,
        };

        iter.seek_to_key(key)?;
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let metas = &self.table.block_meta;
        let mut left = 0;
        let mut right = metas.len();

        while left < right {
            let mid = left + (right - left) / 2;
            let mid_first_key = &metas[mid].first_key;

            if mid_first_key.as_key_slice() <= key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        self.blk_idx = if left == 0 { 0 } else { left - 1 };
        let block = self.table.read_block_cached(self.blk_idx)?;
        self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);

        // 如果在当前的block没有这个key，说明可能在下一个block里面
        if !self.blk_iter.is_valid() && self.blk_idx + 1 < self.table.num_of_blocks() {
            self.blk_idx += 1;
            let next_block = self.table.read_block_cached(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(next_block);
        }

        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&'_ self) -> KeySlice<'_> {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();

        if !self.blk_iter.is_valid() && self.blk_idx + 1 < self.table.num_of_blocks() {
            self.blk_idx += 1;
            let next_block = self.table.read_block_cached(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(next_block);
        }

        Ok(())

    }
}
