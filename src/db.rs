use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    usize,
};

use key::{KeySlice, ValueSlice};

mod store;

mod block;
mod common;
mod db_meta;
mod key;
mod level;
mod logfile;
mod lsm_storage;
mod memtable;
mod snapshot;
mod table;
use db_meta::{DBMeta, ThreadDbMeta};
use logfile::LogFile;
use lsm_storage::LsmStorage;
use store::Store;

struct LsmDB<T: Store> {
    meta: ThreadDbMeta<T>,
    current: lsm_storage::LsmStorage<T>,
    logfile: Arc<Mutex<LogFile<T>>>,
}

impl<T: Store> LsmDB<T> {
    pub fn new(dir: PathBuf) -> Self {
        unimplemented!()
    }
    pub fn open(dir: PathBuf) -> Self {
        unimplemented!()
    }
    pub fn get_snapshot(&self) -> LsmStorage<T> {
        unimplemented!()
    }
    fn compact_thread(meta: Arc<Mutex<DBMeta<T>>>, level: level::LevelStorege<T>) {}

    fn start_compact_thread() {}
}

pub struct DBWriter<T: Store> {
    logfile: Arc<Mutex<LogFile<T>>>,
    current: lsm_storage::LsmStorage<T>,
}
impl<T: Store> DBWriter<T> {
    pub fn write_batch(&mut self, kvs: Vec<(&KeySlice, &ValueSlice)>) {}
}
#[cfg(test)]
mod test {}
