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

use std::ops::Bound;

use anyhow::Result;
use bytes::Bytes;

use crate::{
    iterators::{
        StorageIterator, concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
// type LsmIteratorInner = MergeIterator<MemTableIterator>;
type LsmIteratorInner = TwoMergeIterator<
    // memtables + L0 SSTs
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    // SstConcatIterator, // L1 SSTs
    MergeIterator<SstConcatIterator>, // All levels SSTs
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        Ok(Self {
            inner: iter,
            end_bound,
        })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        // self.inner.is_valid()

        match &self.end_bound {
            Bound::Included(end_key) => {
                self.inner.is_valid() && self.inner.key().into_inner() <= end_key
            }
            Bound::Excluded(end_key) => {
                self.inner.is_valid() && self.inner.key().into_inner() < end_key
            }
            Bound::Unbounded => self.inner.is_valid(),
        }
    }

    fn key(&self) -> &[u8] {
        self.inner.key().into_inner()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        // TODO: I dont know if I need use ? to return the Error or not
        self.inner.next()?;
        // 跳过已经删除的key
        while self.inner.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
        }

        // 检查是否越界
        if self.inner.is_valid() {
            match &self.end_bound {
                Bound::Included(end_key) => {
                    if self.inner.key().into_inner() > end_key {
                        // 超过了end_bound
                        return Ok(());
                    }
                }
                Bound::Excluded(end_key) => {
                    if self.inner.key().into_inner() >= end_key {
                        // 超过了end_bound
                        return Ok(());
                    }
                }
                Bound::Unbounded => {}
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
/// 这个就是一个对当前迭代器的包装器，防止用户在迭代器无效的时候调用next
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        // self.iter.is_valid() && !self.has_errored
        if self.has_errored {
            false
        } else {
            self.iter.is_valid()
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        // 按照测试用例，出现错误就要返回err
        if self.has_errored {
            return Err(anyhow::anyhow!("iterator has errored previously"));
        }

        // 但是对于迭代器已经失效，不应该报错
        if !self.iter.is_valid() {
            return Ok(());
        }

        //
        match self.iter.next() {
            Ok(()) => Ok(()),
            Err(e) => {
                self.has_errored = true;
                Err(e)
            }
        }
    }

    fn num_active_iterators(&self) -> usize {
        if self.has_errored {
            0
        } else {
            self.iter.num_active_iterators()
        }
    }
}
