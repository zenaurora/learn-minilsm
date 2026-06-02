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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            data: Vec::with_capacity(block_size),
            offsets: Vec::new(),
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // 计算添加这个 entry 后的总大小
        // let entry_size = 2 + key.len() + 2 + value.len(); // key_len + key + value_len + value
        // let new_data_size = self.data.len() + entry_size;
        let new_offsets_size = (self.offsets.len() + 1) * 2; // 新增一个 offset
        // let total_size = new_data_size + new_offsets_size + 2; // +2 for num_of_elements

        let overlap_len = if self.first_key.is_empty() {
            0
        } else {
            self.first_key
                .raw_ref()
                .iter()
                .zip(key.into_inner().iter())
                .take_while(|&(a, b)| *a == *b)
                .count()
        };
        let rest_len = key.len() - overlap_len;

        let estimated_size = self.data.len()
            + rest_len
            + std::mem::size_of::<u16>() * 3
            + new_offsets_size
            + 2
            + value.len();

        // 如果不是第一个 entry,且总大小会超过 block_size,返回 false
        if !self.is_empty() && estimated_size > self.block_size {
            return false;
        }

        let key_len = key.len() as u16;
        let value_len = value.len() as u16;
        self.offsets.push(self.data.len() as u16); // 记录当前 data 的偏移量,也是 entry 的起始位置

        // // 一开始用的extend_from_slice,使用BufMut里面的方法更简洁
        // self.data.put_u16_le(key.len() as u16); // 写入 u16 小端序
        // self.data.put_slice(key.into_inner()); // 写入 slice
        // self.data.put_u16_le(value.len() as u16); // 写入 u16 小端序
        // self.data.put_slice(value); // 写入 slice

        self.data.put_slice(self.compact_key(key).raw_ref());
        self.data.put_u16_le(value_len);
        self.data.put_slice(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
            // println!(
            //     "save first_key: {:?}",
            //     String::from_utf8_lossy(self.first_key.raw_ref())
            // );
        }
        true
    }

    fn compact_key(&self, key: KeySlice) -> KeyVec {
        // if the first key is empty, meaning this is the first key
        // just set overlap_len = 0, rest_len = key.len()
        // and return the compacted key
        if self.first_key.is_empty() {
            let mut compacted_key = vec![];
            compacted_key.put_u16_le(0 as u16);
            compacted_key.put_u16_le(key.len() as u16);
            compacted_key.put_slice(key.into_inner());
            return KeyVec::from_vec(compacted_key);
        }

        let overlap_len = self
            .first_key
            .raw_ref()
            .iter()
            .zip(key.into_inner().iter())
            .take_while(|&(a, b)| *a == *b)
            .count();

        let rest_key = &key.into_inner()[overlap_len..];

        let rest_len = rest_key.len() as u16;

        // println!(
        //     "Compacting key: overlap_len={}, rest_len={}",
        //     overlap_len, rest_len
        // );

        let mut compacted_key = vec![];
        compacted_key.put_u16_le(overlap_len as u16);
        compacted_key.put_u16_le(rest_len);
        compacted_key.put_slice(rest_key);

        KeyVec::from_vec(compacted_key)
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        if self.data.is_empty() && self.offsets.is_empty() {
            return true;
        }
        false
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
