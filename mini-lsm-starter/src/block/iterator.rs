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

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        let data_pos = iter.block.offsets[0] as usize;
        let overlap_len =
            u16::from_le_bytes([iter.block.data[data_pos], iter.block.data[data_pos + 1]]) as usize;
        let rest_len =
            u16::from_le_bytes([iter.block.data[data_pos + 2], iter.block.data[data_pos + 3]])
                as usize;
        assert_eq!(overlap_len, 0, "first key overlap_len should be 0");
        let rest_key = &iter.block.data[data_pos + 4..data_pos + 4 + rest_len];
        // println!(
        //     "Initializing first_key {}",
        //     String::from_utf8_lossy(rest_key)
        // );
        iter.first_key = KeyVec::from_vec(rest_key.to_vec());

        iter.seek_to_first();

        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);

        let data_pos = iter.block.offsets[0] as usize;
        let overlap_len =
            u16::from_le_bytes([iter.block.data[data_pos], iter.block.data[data_pos + 1]]) as usize;
        let rest_len =
            u16::from_le_bytes([iter.block.data[data_pos + 2], iter.block.data[data_pos + 3]])
                as usize;
        // assert_eq!(overlap_len, 0, "first key overlap_len should be 0");
        let rest_key = &iter.block.data[data_pos + 4..data_pos + 4 + rest_len];
        // println!(
        //     "Initializing first_key {}",
        //     String::from_utf8_lossy(rest_key)
        // );
        iter.first_key = KeyVec::from_vec(rest_key.to_vec());

        iter.seek_to_key(key);

        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        self.get_value_at_index(self.idx)
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let data = &self.block.data;

        if !data.is_empty() {
            let first_key = self.get_key_at_index(0).raw_ref().to_vec();
            self.key = KeyVec::from_vec(first_key);
            self.first_key = self.key.clone();
            self.idx = 0;
        }
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 < self.block.offsets.len() {
            self.idx += 1;
            self.key = self.get_key_at_index(self.idx);
        } else {
            self.idx = self.block.offsets.len();
            self.key.clear();
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let offsets = &self.block.offsets;

        let mut left = 0;
        let mut right = offsets.len();
        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = self.get_key_at_index(mid);
            if mid_key.as_key_slice() < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // now left is the index
        if left < offsets.len() {
            // println!("seek_to_key: key found at index {}", left);
            self.idx = left;
            self.key = self.get_key_at_index(left);
        } else {
            // not found
            // println!("seek_to_key: key not found");
            self.idx = offsets.len();
            self.key.clear();
        }
    }

    fn get_key_at_index(&self, index: usize) -> KeyVec {
        let data_pos = self.block.offsets[index] as usize;
        // if index != 0 {
        // not the first key, need to decode compacted key
        // update : Now I have set each key as compacted key, so all keys need to be decoded this way
        // we can know if rest_len == 0 then it's the first key
        // no I cant set rest_len == 0 to check. This all WRONG
        // because iter dont know where is the first key.
        // first key should be set on the first place of the data part of block.
        // even BlcokBuilder has first_key part, but it is used to set BlockMeta of SSTable.
        // because SSTable has many blocks.
        // Block struct only has key and value pair.
        let overlap_len =
            u16::from_le_bytes([self.block.data[data_pos], self.block.data[data_pos + 1]]) as usize;

        let rest_len =
            u16::from_le_bytes([self.block.data[data_pos + 2], self.block.data[data_pos + 3]])
                as usize;

        let rest_key = &self.block.data[data_pos + 4..data_pos + 4 + rest_len];

        let mut key = self.first_key.raw_ref()[..overlap_len].to_vec();

        key.extend_from_slice(rest_key);
        // println!("Decoded key: {}", String::from_utf8_lossy(&key));
        KeyVec::from_vec(key)
    }

    fn get_value_at_index(&self, index: usize) -> &[u8] {
        let data = &self.block.data;
        let data_pos = self.block.offsets[index] as usize;

        // 1. 跳过 overlap_len (2 bytes)
        // 2. 读取 rest_key_len (2 bytes)
        let rest_len = u16::from_le_bytes([data[data_pos + 2], data[data_pos + 3]]) as usize;

        // 3. value_len 位于 rest_key 之后
        let value_len_pos = data_pos + 4 + rest_len;
        let value_len = u16::from_le_bytes([data[value_len_pos], data[value_len_pos + 1]]) as usize;

        // 4. value 位于 value_len 之后
        let value_pos = value_len_pos + 2;
        &data[value_pos..value_pos + value_len]
    }
}
