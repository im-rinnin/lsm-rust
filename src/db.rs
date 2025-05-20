mod store;

mod common;
mod db_meta;
mod key;
mod level;
mod logfile;
mod memtable;
mod snapshot;
mod sstable;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use common::*;
use logfile::LogFile;
use memtable::Memtable;
use snapshot::Snapshot;
use sstable::table::TableReader;
use store::Filestore;
use store::Memstore;
use store::Store;

pub struct Config {
    block_size: usize,
    sstable_size: usize,
    level_factor: usize,
    first_level_sstable_num: usize,
}

pub struct DB<T: Store> {
    m: Memtable,
    // todo! immutable memtable
    //vec[n]: level n
    levels: Vec<level::Level<T>>,
    // sstable not need by db, but maybe other snapshot need it, delete it if reference count is 1  (no snapshot need it)
    unactive_sstables: Sender<Arc<TableReader<T>>>,
    logfile: LogFile<T>,
    meta: db_meta::DBMeta<T>,
}

impl<T: Store> DB<T> {
    pub fn new_memstore() -> DB<Memstore> {
        unimplemented!()
    }
    pub fn new_filestore(dir_path: String) -> DB<Filestore> {
        unimplemented!()
    }
    pub fn open_filestore(path: String) -> DB<Filestore> {
        unimplemented!()
    }

    pub fn insert(&mut self, k: &str, v: &str) -> Result<()> {
        // todo check key valuse size
        unimplemented!()
    }

    pub fn snapshot(&self) -> Snapshot<T> {
        unimplemented!()
    }

    pub fn close(self) -> T {
        unimplemented!()
    }
}
