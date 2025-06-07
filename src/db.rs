use std::{
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc, Mutex},
    usize,
};

use common::{KVOpertion, OpId};
use key::{KeyBytes, KeySlice, KeyVec, ValueByte, ValueSlice};

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
use level::TableChangeLog;
use logfile::LogFile;
use lsm_storage::LsmStorage;
use store::{Store, StoreId};

struct LsmDB<T: Store> {
    current_max_op_id: AtomicU64,
    current_max_sstable_id: StoreId, // dont need to save to file
    current: Arc<Mutex<Arc<LsmStorage<T>>>>,
    logfile: Arc<Mutex<LogFile<T>>>,
    level_meta_log: Arc<Mutex<TableChangeLog<T>>>,
}

impl<T: Store> LsmDB<T> {
    pub fn new(dir: PathBuf) -> Self {
        unimplemented!()
    }
    pub fn open(dir: PathBuf) -> Self {
        unimplemented!()
    }
    pub fn get_reader(&self) -> DBReader<T> {
        unimplemented!()
    }
    pub fn get_writer(&self) -> DBWriter<T> {
        unimplemented!()
    }

    fn check_memtable_size(&self) {}
    fn dump_and_compact_thread(lsm: LsmStorage<T>) {
        // loop
        // sleep
        // check if db end, return
        // check metable size
        // dump memtable if needed
        // update current and meta
        // check level zeor size
        // do compact if needed
        // update current and meta
        // back to loop
    }

    fn start_compact_thread(&self) {}
}

pub struct DBWriter<T: Store> {
    logfile: Arc<Mutex<LogFile<T>>>,
    current: lsm_storage::LsmStorage<T>,
}
impl<T: Store> DBWriter<T> {
    pub fn write_batch(&mut self, kvs: Vec<(&KeyBytes, &ValueByte)>) -> OpId {
        unimplemented!()
    }
    pub fn write(&mut self, kvs: Vec<(&KeySlice, &ValueSlice)>) -> OpId {
        unimplemented!()
    }
}
pub struct DBReader<T: Store> {
    current: Arc<LsmStorage<T>>,
}
impl<T: Store> DBReader<T> {
    pub fn query(&self, key: KeyVec) -> Option<KVOpertion> {
        unimplemented!()
    }
}
#[cfg(test)]
mod test {}
