#![allow(unused)]
use std::sync::atomic::Ordering;
use std::{
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc, Condvar, Mutex},
    usize,
};

use common::{KVOpertion, OpId};
use key::{KeyBytes, KeySlice, KeyVec, ValueByte, ValueSlice, ValueVec};

mod store;

mod block;
mod common;
mod db_meta;
mod key;
mod level;
use crossbeam_channel::{Receiver, Sender};
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
    current_max_op_id: Arc<AtomicU64>,
    current_max_sstable_id: AtomicU64, // dont need to save to file
    // The Condvar/Mutex pair for signaling write completion.
    write_done_cv: Arc<Mutex<Arc<(Condvar, Mutex<bool>)>>>,
    current: Arc<Mutex<Arc<LsmStorage<T>>>>,
    logfile: Arc<Mutex<LogFile<T>>>,
    level_meta_log: Arc<Mutex<TableChangeLog<T>>>,
    request_queue_r: Receiver<KVOpertion>,
    request_queue_s: Sender<KVOpertion>,
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
    pub fn get_writer(&self) -> DBWriter {
        DBWriter {
            max_op: self.current_max_op_id.clone(),
            queue: self.request_queue_s.clone(),
            write_done_cv: self.write_done_cv.clone(),
        }
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

            let queue_len = self.request_queue_r.len();
            if queue_len < START_WRITE_REQUEST_QUEUE_LEN {
                continue;
            } // Needs constant definition

            // CV swapping logic: Clone the old CV, replace with a new one
            let old_cv_pair; // To hold the CV pair for notification
            {
                let mut cv_guard = self.write_done_cv.lock().unwrap();
                // Clone the current Arc to notify waiters later
                old_cv_pair = cv_guard.clone();
                // Create and set a new CV pair for future writers
                let new_cv_pair = Arc::new((Condvar::new(), Mutex::new(false)));
                *cv_guard = new_cv_pair;
                // MutexGuard is dropped here, releasing the lock
            }

            let mut ops_to_write = Vec::new();
            // Drain the queue or receive multiple items
            while let Ok(op) = self.request_queue_r.try_recv() {
                ops_to_write.push(op);
                // Potentially break if too many ops are collected or after a timeout
            }

            if ops_to_write.is_empty() {
                // If no ops were received, we don't need to notify anyone with the old CV.
                // The new CV is already in place for the next batch.
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

            // Notify client threads using the old CV pair that was cloned earlier
            let (cv, lock) = &*old_cv_pair;
            let mut completed = lock.lock().unwrap();
            *completed = true;
            cv.notify_all();
        }
        // Note: The loop as written never exits. A mechanism to stop the worker is needed.
    }
}

pub struct DBWriter {
    max_op: Arc<AtomicU64>,
    queue: Sender<KVOpertion>,
    write_done_cv: Arc<Mutex<Arc<(Condvar, Mutex<bool>)>>>,
}
impl DBWriter {
    pub fn write(&mut self, kvs: Vec<(&KeyVec, &ValueVec)>) -> OpId {
        // Fetch current max op id and increment by kvs len
        let start_op_id = self.max_op.fetch_add(kvs.len() as u64, Ordering::SeqCst);
        let end_op_id = start_op_id + kvs.len() as u64;

        // Get and lock the current write_done_cv pair
        let cv_pair_arc = {
            let cv_guard = self.write_done_cv.lock().unwrap();
            cv_guard.clone() // Clone the Arc to hold onto the specific CV pair
        };
        let (cv, lock) = &*cv_pair_arc;

        // Prepare the completed flag for waiting
        let mut completed = lock.lock().unwrap();
        assert!(!*completed);

        // Submit write requests to the queue
        for (i, (key, value)) in kvs.into_iter().enumerate() {
            let op_id = start_op_id + i as u64;
            let op = KVOpertion::new(
                op_id,
                key.clone(),
                common::OpType::Write(ValueByte::from(value.as_ref())),
            );
            self.queue
                .send(op)
                .expect("Failed to send KVOpertion to queue");
        }

        // Wait for the worker thread to complete the write operation
        while !*completed {
            completed = cv.wait(completed).unwrap();
        }

        // Return the max op_id in this write operation
        end_op_id - 1 // The last OpId in the range [start_op_id, end_op_id)
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
