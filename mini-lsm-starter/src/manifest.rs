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

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Read};

use anyhow::{Context, Result};
use bytes::Buf;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        // use OpenOptions!
        // match File::open(&path) {
        //     Ok(file) => {
        //         File::sync_all(&file)?;
        //         return Ok(Manifest {
        //             file: Arc::new(Mutex::new(file)),
        //         });
        //     }
        //     Err(e) => {
        //         let file = File::create(&path)?;
        //         File::sync_all(&file)?;
        //         return Ok(Manifest {
        //             file: Arc::new(Mutex::new(file)),
        //         });
        //     }
        // }
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create manifest")?,
            )),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        // let manifest = Manifest::create(&path)?;
        // let mut file = File::open(&path)?;

        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("fail to revover manifest")?;
        let mut buf = Vec::new();
        let byte_len = file.read_to_end(&mut buf)?;

        let mut records: Vec<ManifestRecord> = Vec::new();

        let stream = serde_json::Deserializer::from_slice(&buf);

        for record in stream.into_iter() {
            records.push(record?);
        }

        Ok((
            Manifest {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {

        let mut file = self.file.lock();
        let record_u8 = serde_json::to_vec(&record)?;

        file.write_all(&record_u8)?;
        file.sync_all()?;
        Ok(())
    }
}
