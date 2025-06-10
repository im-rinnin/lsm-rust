#![allow(unused)]
use std::{
    path::PathBuf,
    sync::{atomic::AtomicU64, mpsc::Receiver, Arc, Condvar, Mutex},
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
    current_max_sstable_id: AtomicU64, // dont need to save to file
    write_done_cv: Arc<Mutex<Option<Arc<(Condvar, Mutex<bool>)>>>>,
    current: Arc<Mutex<Arc<LsmStorage<T>>>>,
    logfile: Arc<Mutex<LogFile<T>>>,
    level_meta_log: Arc<Mutex<TableChangeLog<T>>>,
    request_queue: Receiver<KVOpertion>,
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

    fn write_worker(&mut self) {
        // sleep
        // check queue len()
        // continue sleep it queue not reach START_WRITE_REQUEST_QUEUE_LEN
        // take cv out and set a new cv to &self
        // get request from queue, write them to logfile first and then write to lsm memtable
        // notify client thread by cv
        unimplemented!()
    }
}

pub struct DBWriter<T: Store> {
    max_op: AtomicU64,
    logfile: Arc<Mutex<LogFile<T>>>,
}
impl<T: Store> DBWriter<T> {
    pub fn write_batch(&mut self, kvs: Vec<(&KeyBytes, &ValueByte)>) -> OpId {
        unimplemented!()
    }
    pub fn write(&mut self, kvs: Vec<(&KeySlice, &ValueSlice)>) -> OpId {
        unreachable!()
        // * fetch and increase max op id by kvs len
        // * write to log file  (lock and release)
        // * Write to memtable.
        // return max op id in kvs
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
