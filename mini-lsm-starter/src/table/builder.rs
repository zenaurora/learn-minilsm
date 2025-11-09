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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::{BlockMeta, SsTable};
use crate::{
    block::BlockBuilder,
    key::{self, KeySlice},
    lsm_storage::BlockCache,
    table::{FileObject, bloom::Bloom},
};

use core::hash::Hasher;
use farmhash;
use farmhash::FarmHasher;
use farmhash::fingerprint32;
/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::with_capacity(block_size),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        let is_added = self.builder.add(key, value);

        if is_added {
            self.key_hashes.push(fingerprint32(key.into_inner()));

            if self.first_key.is_empty() {
                self.first_key.put_slice(key.into_inner());
            } else {
                self.last_key.clear();
                self.last_key.put_slice(key.into_inner());
                // println!(
                //     "Updating last_key: {:?}",
                //     String::from_utf8_lossy(&self.last_key)
                // );
            }
        } else {
            // Current block is full, need to finish it and start a new one.

            // a bug fixed after 1 hours:
            // I forget to set last_key when finishing a block with only one key-value pair
            // at first I met a problem before is forget to clear first_key after finishing a block,
            if self.last_key.is_empty() {
                self.last_key.put_slice(self.first_key.as_slice());
            }
            let block_meta = BlockMeta {
                first_key: key::Key::from_bytes(Bytes::copy_from_slice(&self.first_key)),
                last_key: key::Key::from_bytes(Bytes::copy_from_slice(&self.last_key)),
                offset: self.data.len(),
            };
            self.meta.push(block_meta);

            // Finish the current block and get its data
            let old_builder =
                std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            let block_data = old_builder.build().encode();
            self.data.extend_from_slice(&block_data);
            /* a bug fixed after 1 hours:
               I forget to reset first_key and last_key after finishing a block,
               so the new block will have wrong first_key and last_key.
               It will always be the first_key and last_key of the previous block.
            */
            self.first_key.clear();
            self.last_key.clear();
            // just recursively call add
            self.add(key, value);
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let block = self.builder.build();

        if self.last_key.is_empty() {
            self.last_key.put_slice(&self.first_key);
            // println!(
            //     "Updating last_key to solve>>: {:?}",
            //     String::from_utf8_lossy(&self.last_key)
            // );
        }
        self.meta.push(BlockMeta {
            first_key: key::Key::from_bytes(Bytes::copy_from_slice(&self.first_key)),
            last_key: key::Key::from_bytes(Bytes::copy_from_slice(&self.last_key)),
            offset: self.data.len(),
        });

        self.data.extend_from_slice(&block.encode());

        let meta_offset = self.data.len();

        // encode block meta
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        /*
        -----------------------------------------------------------------------------------------------------
        |         Block Section         |                            Meta Section                           |
        -----------------------------------------------------------------------------------------------------
        | data block | ... | data block | metadata | meta block offset | bloom filter | bloom filter offset |
        |                               |  varlen  |         u32       |    varlen    |        u32          |
        -----------------------------------------------------------------------------------------------------
        */

        // meta block offset(u32)
        self.data
            .extend_from_slice(&(meta_offset as u32).to_le_bytes());

        // calculate bits per key based on length and false positive
        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom_filter = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);
        let bloom_filter_offset = self.data.len();
        // put bloom filter after the meta offset
        bloom_filter.encode(&mut self.data);

        // bloom filter offset(u32)
        self.data
            .put_slice(&(bloom_filter_offset as u32).to_le_bytes());

        let file = FileObject::create(path.as_ref(), self.data)?;

        let first_key = self.meta.first().unwrap().first_key.clone();
        let last_key = self.meta.last().unwrap().last_key.clone();

        Ok(SsTable {
            id,
            file,
            first_key,
            last_key,
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            block_cache,
            bloom: Some(bloom_filter),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
