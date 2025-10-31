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

mod builder;
mod iterator;

use std::io::{Cursor, Seek};

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        // buf.extend_from_slice(&self.data);
        for offset in &self.offsets {
            buf.put_u16_le(*offset);
        }
        let num_of_elements = self.offsets.len() as u16;
        buf.put_u16_le(num_of_elements);

        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let data_len = data.len();
        let element_nums = u16::from_le_bytes([data[data_len - 2], data[data_len - 1]]) as usize;

        let offset_start = data_len - element_nums * 2 - 2;

        let data_vec = data[..offset_start].to_vec();

        let mut offset_vec = Vec::new();

        for i in 0..element_nums {
            let start_position = offset_start + i * 2;

            let offset = u16::from_le_bytes([data[start_position], data[start_position + 1]]);

            offset_vec.push(offset);
        }

        Self {
            data: data_vec,
            offsets: offset_vec,
        }
    }
}
