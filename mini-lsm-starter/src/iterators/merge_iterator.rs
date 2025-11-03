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

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse() // 反转，实现让小的在堆顶部
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    // 待处理的堆,里面也是迭代器，每个都是(usize, Box<I>),iter.1就是真正的那个usize对应的memtable的迭代器
    // 由于最新的iter.0最小，因此拿到的就是最新的数据
    current: Option<HeapWrapper<I>>,
    // 当前的迭代器,这个是真正要使用的
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        /*
           把所有的iter，只要有效，就放在堆里
           然后最后从堆中弹出一个有效的iter作为current
        */
        let mut heap = BinaryHeap::new();
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        let mut current = None;
        while let Some(candidate) = heap.pop() {
            // 如果还有效,就作为 current
            if candidate.1.is_valid() {
                current = Some(candidate);
                break;
            }
            // 否则继续尝试下一个
        }

        Self {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
        // unimplemented!()
    }

    fn next(&mut self) -> Result<()> {
        // let current = self.current.as_mut().unwrap(); // 当前的迭代器
        let Some(mut current) = self.current.take() else {
            return Ok(());
        };

        let current_key = current.1.key().to_key_vec(); // 需要clone一下，否则会遇到所有权问题
        // 先把当前的迭代器往下走一步
        // current.1.next 本质是 *current.1, 是一个可变借用
        if let Err(e) = current.1.next() {
            self.current = None;
            return Err(e);
        } else {
            // 如果当前的迭代器还是有效的，就把它放回堆里
            if current.1.is_valid() {
                self.iters.push(current);
            }
        }

        while let Some(mut heap_top) = self.iters.peek_mut() {
            if !heap_top.1.is_valid() {
                // 如果当前迭代器失效了，就把它弹出
                PeekMut::pop(heap_top);
                continue;
            }

            if current_key.as_key_slice() == heap_top.1.key() {
                if let Err(e) = heap_top.1.next() {
                    PeekMut::pop(heap_top);
                    return Err(e);
                } else {
                    if !heap_top.1.is_valid() {
                        PeekMut::pop(heap_top);
                    }
                }
            } else {
                break;
            }
        }
        // 弹出新的最小 key 对应的那个迭代器，作为 current
        while let Some(candidate) = self.iters.pop() {
            if candidate.1.is_valid() {
                self.current = Some(candidate);
                return Ok(());
            }
        }

        self.current = None;
        Ok(())
    }
    /*
    深入理解合并原理；
    1.  首先多个memtable的iter都被放在一个堆中
        其中先按照key的大小排序，再按照memtable的index排序
        这样就保证了堆顶的元素是当前所有iter中key最小
    2.  current字段指的是当前的iter，但是堆中又很多iter
        需要先保存一下current的key
        再需将current调用一次next，尝试往后走一步，然后再把它放回堆
        实现堆顶的元素是key最小的。
    3.  然后就是从堆顶拿出数据，看一看它的key
        如果和current的key相同，就说明这个key是旧值
        需要将堆顶的iter也调用next，尝试往后走一步
        然后再把它放回堆
        直到堆顶的key和current的key不相同为止
    4.  最后将堆顶的iter弹出，作为新的current
     */

    fn num_active_iterators(&self) -> usize {
        let currnet_nums_of_active = self
            .current
            .as_ref()
            .map(|i| i.1.num_active_iterators())
            .unwrap_or(0);
        self.iters.iter().fold(currnet_nums_of_active, |sum, i| {
            sum + i.1.num_active_iterators()
        })
    }
}
