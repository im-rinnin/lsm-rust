#![allow(unused)]
use std::{
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc, Condvar, Mutex},
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
use crossbeam_channel::Receiver;
mod logfile;
mod lsm_storage;
mod memtable;
mod snapshot;
mod table;
use level::TableChangeLog;
use logfile::LogFile;
use lsm_storage::LsmStorage;
use store::{Store, StoreId};
const START_WRITE_REQUEST_QUEUE_LEN: usize = 100;
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
    fn write_worker(&mut self, stop: Mutex<bool>) {
        // This function needs more context like START_WRITE_REQUEST_QUEUE_LEN,
        // how to handle sleep/wake, and the exact mechanism for CV swapping.
        // The implementation below is a basic structure based on the comments.
        // It assumes a loop and basic locking, but details need refinement.

        loop {
            // Placeholder for sleep logic
            std::thread::sleep(std::time::Duration::from_millis(100)); // Example sleep

            // Check if stop signal is received
            let stop_flag = stop.lock().unwrap();
            if *stop_flag {
                break; // Exit the loop if stop flag is true
            }
            // Drop the guard explicitly after checking the flag
            drop(stop_flag);

            let queue_len = self.request_queue.len();
            if queue_len < START_WRITE_REQUEST_QUEUE_LEN {
                continue;
            } // Needs constant definition

            // Placeholder: CV swapping logic
            let mut cv_guard = self.write_done_cv.lock().unwrap();
            let current_cv_opt = cv_guard.take(); // Take the current CV out
                                                  // Create and set a new CV pair
            let new_cv_pair = Arc::new((Condvar::new(), Mutex::new(false)));
            *cv_guard = Some(new_cv_pair.clone());
            drop(cv_guard); // Release lock before potentially long operations

            let mut ops_to_write = Vec::new();
            // Drain the queue or receive multiple items
            while let Ok(op) = self.request_queue.try_recv() {
                ops_to_write.push(op);
                // Potentially break if too many ops are collected or after a timeout
            }

            if ops_to_write.is_empty() {
                // If no ops were received, restore the original CV if it existed
                if let Some(original_cv) = current_cv_opt {
                    let mut cv_guard = self.write_done_cv.lock().unwrap();
                    *cv_guard = Some(original_cv);
                }
                continue; // Go back to sleep/wait
            }

            // Write to log file
            {
                let mut logfile_guard = self.logfile.lock().unwrap();
                logfile_guard.append(&ops_to_write); // Clone ops for memtable write
            } // Logfile lock released

            // Write to memtable
            {
                let current_lsm_guard = self.current.lock().unwrap();
                for op in ops_to_write {
                    // Assuming LsmStorage has a method like `put` or `insert`
                    // This might need adjustment based on LsmStorage's actual API
                    current_lsm_guard.put(op); // Error handling needed
                }
            } // LsmStorage lock released

            // Notify client threads using the CV that was taken out
            if let Some(cv_pair) = current_cv_opt {
                let (cv, lock) = &*cv_pair;
                let mut completed = lock.lock().unwrap();
                *completed = true;
                cv.notify_all();
            }
        }
        // Note: The loop as written never exits. A mechanism to stop the worker is needed.
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
