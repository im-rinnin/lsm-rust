#![allow(unused)]
use std::default;
use std::sync::atomic::Ordering;
use std::sync::RwLock;
use std::thread::{self, JoinHandle};
use std::{
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc, Condvar, Mutex},
    time::{Duration, Instant}, // Required for wait_timeout and Instant
    usize,
};
mod db_log;

use anyhow::Result;
use common::{KVOpertion, OpId, OpType};
use key::{KeyBytes, KeySlice, KeyVec, ValueByte, ValueSlice, ValueVec};

mod store;

mod block;
mod common;
mod db_meta;
mod key;
mod level;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
mod logfile;
mod lsm_storage;
mod memtable;
mod snapshot;
mod table;
use level::TableChangeLog;
use logfile::LogFile;
use lsm_storage::{LsmStorage, LsmStorageConfig};
use store::{Memstore, Store, StoreId};
use tracing::info;

const START_WRITE_REQUEST_QUEUE_LEN: usize = 100;
const WRITE_TIMEOUT_MS: u128 = 10; // Timeout in milliseconds

struct WriteRequest {
    data: Vec<(KeyBytes, OpType)>,
    done: Sender<OpId>,
}
impl WriteRequest {
    fn new(data: Vec<(KeyBytes, OpType)>) -> (Self, Receiver<OpId>) {
        let (s, r) = bounded(1);
        let res = WriteRequest { data, done: s };
        (res, r)
    }
    pub fn get_size(&self) -> usize {
        let mut total_size = 0;
        for (key, op_type) in &self.data {
            // Size of key data + size of u64 for key length prefix
            let mut item_size = key.len() + std::mem::size_of::<u64>();
            // Size of u8 for OpType discriminant
            item_size += std::mem::size_of::<u8>();
            if let OpType::Write(v) = op_type {
                // Size of value data + size of u64 for value length prefix
                item_size += v.len() + std::mem::size_of::<u64>();
            }
            total_size += item_size;
        }
        total_size
    }
}

#[derive(Default)]
struct DBStatistic {
    pub compact_count: AtomicU64,
}
struct LsmDB<T: Store> {
    current_max_op_id: Arc<AtomicU64>,
    // The current state of the LSM tree, wrapped for concurrent access and modification.
    current: Arc<RwLock<LsmStorage<T>>>,
    request_queue_r: Receiver<WriteRequest>,
    request_queue_s: Sender<WriteRequest>,
    stop_flag: Arc<Mutex<bool>>, // Added to control worker threads shutdown
    thread_handls: Vec<JoinHandle<()>>,
    trigger_compact: Sender<()>,
    statistic: Arc<DBStatistic>,
    config: Config,
}
const SLEEP_TIME: u64 = 10;
#[derive(Clone, Copy)]
pub struct Config {
    pub lsm_storage_config: LsmStorageConfig,
    pub request_queue_len: usize,
    pub imm_size_limit: usize,
    pub thread_sleep_time: u64,
    pub request_queue_size: usize,
    pub auto_compact: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            lsm_storage_config: LsmStorageConfig::default(),
            request_queue_len: 500,
            imm_size_limit: 2,
            thread_sleep_time: 100,
            request_queue_size: 1000,
            auto_compact: true,
        }
    }
}
impl Config {
    fn config_for_test() -> Self {
        let mut c = Self::default();
        c.lsm_storage_config = LsmStorageConfig::config_for_test();
        c.request_queue_size = 200;
        c
    }
}

impl<T: Store> Drop for LsmDB<T> {
    fn drop(&mut self) {
        info!("close db start drop");
        // Signal worker threads to stop
        let mut stop = self.stop_flag.lock().expect("fail to get lock on flah");
        *stop = true;
        drop(stop); // Release the lock before joining threads

        // Wait for all worker threads to finish
        for handle in self.thread_handls.drain(..) {
            let _ = handle.join(); // Ignore join errors for now, consider logging in production
        }
        info!("close db end drop");
    }
}

impl LsmDB<Memstore> {
    pub fn new_with_memstore(config: Config) -> Self {
        // Create the initial LsmStorage, wrapped in Arc<RwLock<...>>
        let initial_lsm_storage = Arc::new(RwLock::new(LsmStorage::new(config.lsm_storage_config)));

        // Create LogFile and TableChangeLog with unique Memstore instances
        let logfile_store_id = 0;
        let logfile = LogFile::<Memstore>::open(logfile_store_id);

        let level_meta_log_store_id = 1; // Use a different ID from logfile
        let level_meta_log = TableChangeLog::<Memstore>::from(level_meta_log_store_id);

        // Create the channel for write requests
        let (s, r) = crossbeam_channel::bounded(config.request_queue_len);

        // Initialize atomic counters
        let current_max_op_id = Arc::new(AtomicU64::new(0));

        // Initialize the write completion signaling mechanism
        let initial_cv_pair = (Condvar::new(), Mutex::new(0));
        let write_done_cv = Arc::new(RwLock::new(initial_cv_pair));

        // Create the stop flag for worker threads
        let stop_flag = Arc::new(Mutex::new(false));

        // Construct the LsmDB instance
        let (trigger_s, trigger_r) = unbounded();
        let mut res = LsmDB {
            current_max_op_id,
            current: initial_lsm_storage, // Use the already wrapped initial state
            request_queue_r: r,
            request_queue_s: s,
            stop_flag: stop_flag.clone(), // Store a clone in the LsmDB instance
            thread_handls: vec![],
            trigger_compact: trigger_s,
            statistic: Arc::new(DBStatistic::default()),
            config,
        };

        // Prepare data needed for the writer thread
        let writer_queue_r = res.request_queue_r.clone();
        let writer_lsm_state = res.current.clone();
        // Note: logfile is moved into the thread closure below.

        let w_stop_flag = stop_flag.clone(); // Clone for writer thread
        let max_op = res.current_max_op_id.clone();
        let h = thread::spawn(move || {
            LsmDB::write_worker(
                w_stop_flag,
                writer_queue_r,
                writer_lsm_state,
                logfile,
                max_op,
                config,
            );
        });
        res.thread_handls.push(h);

        // Prepare data for the compaction thread
        let compaction_lsm_state = res.current.clone();
        let compaction_config = config; // Clone or copy config if needed, struct is Copy
        let mut next_sstable_id_start_value: StoreId = 0; // Start after logfile and meta log
                                                          // todo: load next_sstable_id from storage

        // Start dump_and_compact_thread in a new thread
        let c_stop_flag = stop_flag.clone(); // Clone for compaction thread
        let statistic = res.statistic.clone();
        let h = thread::spawn(move || {
            LsmDB::dump_and_compact_thread(
                compaction_lsm_state,
                level_meta_log,              // level_meta_log is moved into the thread
                next_sstable_id_start_value, // Passed by value, matches 'mut StoreId' signature
                c_stop_flag,
                compaction_config,
                trigger_r,
                statistic,
            );
        });
        res.thread_handls.push(h);

        res
    }

    fn trigger_compact(&mut self) {
        self.trigger_compact.send(()).unwrap();
    }
}

impl<T: Store> LsmDB<T> {
    pub fn new(dir: PathBuf) -> Self {
        unimplemented!()
    }
    pub fn open(dir: PathBuf) -> Self {
        unimplemented!()
    }

    pub fn table_num_in_levels(&self) -> Vec<usize> {
        // Acquire read lock to get the LsmStorage
        let lsm_storage_guard = self.current.read().unwrap();
        lsm_storage_guard.table_num_in_levels()
        // Lock is released when guard goes out of scope
    }

    pub fn get_reader(&self) -> DBReader<T> {
        let cloned_lsm_storage = self.current.read().unwrap().clone();

        let current_op_id = self.current_max_op_id.load(Ordering::SeqCst);
        DBReader {
            // DBReader holds an Arc to the *cloned* LsmStorage state
            snapshot: cloned_lsm_storage,
            id: current_op_id,
        }
    }
    pub fn get_writer(&self) -> DBWriter {
        DBWriter {
            queue: self.request_queue_s.clone(),
        }
    }
    pub fn need_freeze(&self) -> bool {
        let lsm_storage_guard = self.current.read().unwrap();
        lsm_storage_guard.memtable_size() >= self.config.lsm_storage_config.memtable_size_limit
    }
    pub fn memtable_size(&self) -> usize {
        // Acquire read lock to get the LsmStorage
        let lsm_storage_guard = self.current.read().unwrap();
        lsm_storage_guard.memtable_size()
        // Lock is released when guard goes out of scope
    }
    pub fn need_dump(&self) -> bool {
        let lsm_storage_guard = self.current.read().unwrap();
        lsm_storage_guard.immtable_num() > 0
    }
    pub fn imm_memtable_count(&self) -> usize {
        let lsm_storage_guard = self.current.read().unwrap();
        lsm_storage_guard.immtable_num()
    }
    pub fn need_compact(&self) -> bool {
        let lsm_storage_guard = self.current.read().unwrap();
        lsm_storage_guard.need_compact()
    }

    pub fn current_op_id(&self) -> OpId {
        self.current_max_op_id.load(Ordering::SeqCst)
    }

    pub fn print_debug_info(&self) {
        self.current.read().unwrap().log_lsm_debug_info();
    }

    fn check_memtable_size(&self) -> usize {
        // Acquire read lock
        let lsm_storage_guard = self.current.read().unwrap();
        lsm_storage_guard.memtable_size()
        // Lock released
    }

    /// Checks if the stop signal has been received for the compaction thread.
    /// Returns `true` if the thread should stop, `false` otherwise.
    fn check_compact_thread_stop_signal(stop: &Arc<Mutex<bool>>) -> bool {
        let stop_flag_inner = stop.lock().unwrap();
        if *stop_flag_inner {
            info!("thread received stop signal during in-loop compaction.");
            return true; // Signal to exit the loop
        }
        false // Continue operation
    }

    fn dump_and_compact_thread(
        current: Arc<RwLock<LsmStorage<T>>>,
        mut meta: TableChangeLog<Memstore>,
        mut next_sstable_id: StoreId,
        stop: Arc<Mutex<bool>>,
        config: Config,
        run_job: Receiver<()>,
        statistic: Arc<DBStatistic>,
    ) {
        loop {
            if Self::check_compact_thread_stop_signal(&stop) {
                break; // Exit the inner loop and then the outer loop
            }
            // Wait for a manual compaction trigger or timeout
            let res = run_job.recv_timeout(Duration::from_millis(config.thread_sleep_time));
            match res {
                Ok(()) => {
                    info!("receive manual compact");
                    // Manual compaction triggered, proceed with compaction logic below
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if !config.auto_compact {
                        continue;
                    }
                    info!("timeout start compact");
                    // time out , proceed with compaction logic below
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    break; // Exit loop if the trigger mechanism is broken.
                }
            }

            // Perform compaction and loop immediately if more compaction is needed
            loop {
                if Self::check_compact_thread_stop_signal(&stop) {
                    break; // Exit the inner loop and then the outer loop
                }

                let mut lsm_storage_clone = current.read().unwrap().clone();
                info!("dump and compact ");
                lsm_storage_clone.log_lsm_debug_info();
                // Dump all immutable memtables until none are left
                if lsm_storage_clone.immtable_num() > 0 {
                    lsm_storage_clone.dump_imm_memtable(&mut next_sstable_id);
                }

                let table_changes = lsm_storage_clone.compact_level(&mut next_sstable_id);
                if table_changes.len() > 0 {
                    meta.append(table_changes); // Persist changes to the TableChangeLog
                }

                info!("after a compact cycle");
                lsm_storage_clone.log_lsm_debug_info();

                let need_compact = lsm_storage_clone.need_compact();
                // Update current LsmStorage state
                *current.write().unwrap() = lsm_storage_clone;
                statistic.compact_count.fetch_add(1, Ordering::SeqCst);
                info!("compact finish");
                if !need_compact {
                    break;
                }
            }
        }
    }

    pub fn depth(&self) -> usize {
        self.table_num_in_levels().len()
    }
    fn tables_count(&self) -> usize {
        // Acquire read lock
        let lsm_storage_guard = self.current.read().unwrap();
        lsm_storage_guard.table_num_in_levels().iter().sum()
        // Lock released
    }
    fn write_worker(
        stop: Arc<Mutex<bool>>,
        request_queue_r: Receiver<WriteRequest>,
        current_lsm: Arc<RwLock<LsmStorage<T>>>,
        mut logfile: LogFile<T>,
        mut next_op_id: Arc<AtomicU64>,
        c: Config,
    ) {
        // This function needs more context like START_WRITE_REQUEST_QUEUE_LEN,
        // how to handle sleep/wake, and the exact mechanism for CV swapping.
        // The implementation below is a basic structure based on the comments.
        // It assumes a loop and basic locking, but details need refinement.

        let mut first_request_time: Option<Instant> = None; // Track time of first request in batch window
        let mut buffered_requests: Vec<WriteRequest> = Vec::new(); // Buffer for incoming requests
        let mut total_buffered_size = 0; // Total size of requests in buffered_requests

        loop {
            if Self::check_compact_thread_stop_signal(&stop) {
                break; // Exit the loop if stop signal is true
            }

            // Try to receive and buffer new requests
            while let Ok(request) = request_queue_r.try_recv() {
                total_buffered_size += request.get_size();
                buffered_requests.push(request);
            }

            let proceed_to_write = Self::should_proceed_to_write_logic(
                total_buffered_size,
                &mut first_request_time,
                c,
            );

            if !proceed_to_write {
                // If conditions for writing are not met, sleep before checking again
                std::thread::sleep(std::time::Duration::from_millis(SLEEP_TIME));
                continue; // Skip the write phase for this iteration
            }

            // --- Proceed with writing ---
            // Process all buffered requests
            // Use `drain(..)` to process and empty the buffer
            for request in buffered_requests.drain(..) {
                Self::process_single_buffered_request(
                    request,
                    &mut logfile,
                    &current_lsm,
                    &next_op_id,
                );
            }
            total_buffered_size = 0; // Reset total size after processing the batch
            first_request_time = None;

            // Freeze memtable if it reaches the size limit
            {
                let mut lsm_storage_guard = current_lsm.write().unwrap();
                if lsm_storage_guard.memtable_size() >= c.lsm_storage_config.memtable_size_limit {
                    info!("Memtable size exceeded in write worker, freezing memtable.");
                    lsm_storage_guard.freeze_memtable();

                    // Check immutable memtable count and wait if it exceeds the limit
                    while lsm_storage_guard.immtable_num() >= c.imm_size_limit {
                        drop(lsm_storage_guard); // Release write lock before sleeping
                        info!(
                            "Too many immutable memtables ({}), waiting for compaction...",
                            c.imm_size_limit
                        );
                        std::thread::sleep(std::time::Duration::from_millis(c.thread_sleep_time));
                        lsm_storage_guard = current_lsm.write().unwrap(); // Re-acquire lock
                        if Self::check_compact_thread_stop_signal(&stop) {
                            break; // Exit the loop if stop signal is true
                        }
                    }
                }
            }
            // No sleep here, as work was done, we can immediately check for more work
        }
        // Note: The loop as written never exits. A mechanism to stop the worker is needed.
    }

    fn process_single_buffered_request(
        request: WriteRequest,
        logfile: &mut LogFile<T>,
        current_lsm_state: &Arc<RwLock<LsmStorage<T>>>,
        next_op_id: &Arc<AtomicU64>,
    ) {
        let (kvs, finishi) = (request.data, request.done);

        if kvs.is_empty() {
            let _ = finishi.send(next_op_id.load(Ordering::SeqCst)); // Send current max OpId as no new ops were written
            return;
        }

        let mut ops_to_write = Vec::new();
        let mut current_id = next_op_id.load(Ordering::SeqCst);
        for kv in kvs {
            ops_to_write.push(KVOpertion {
                id: current_id,
                key: kv.0,
                op: kv.1,
            });
            current_id += 1;
        }
        logfile.append(&ops_to_write);

        {
            let lsm_storage_guard = current_lsm_state.read().unwrap();
            for op in ops_to_write {
                lsm_storage_guard.put(op);
            }
        }
        next_op_id.store(current_id, Ordering::SeqCst);
        let _ = finishi.send(current_id - 1); // Send the last OpId of the processed batch
    }

    /// Determines whether the write worker should proceed with writing a batch of requests.
    /// It considers the queue length and a timeout for pending requests.
    /// Returns a tuple: (should_write, updated_first_request_time).
    fn should_proceed_to_write_logic(
        request_size: usize,
        mut first_request_time: &mut Option<Instant>, // Pass mutable Option<Instant>
        c: Config,
    ) -> bool {
        let mut proceed_to_write = false;
        // Use the passed-in first_request_time
        if request_size >= c.request_queue_size {
            proceed_to_write = true;
            *first_request_time = None; // Reset timer when writing due to queue length
        } else if request_size > 0 {
            // Only check time if there's something in the queue
            match first_request_time {
                None => {
                    // First request arrived since last write, start the timer
                    *first_request_time = Some(Instant::now());
                }
                Some(start_time) => {
                    // Timer already running, check if timeout reached
                    if start_time.elapsed().as_millis() >= WRITE_TIMEOUT_MS {
                        proceed_to_write = true;
                        *first_request_time = None; // Reset timer after writing due to timeout
                    }
                }
            }
        } else {
            // Queue is empty, reset the timer
            *first_request_time = None;
        }
        proceed_to_write
    }
}
pub struct DBWriter {
    queue: Sender<WriteRequest>,
}
impl DBWriter {
    pub fn put(&mut self, kvs: Vec<(KeyBytes, OpType)>) -> Receiver<OpId> {
        let (request, r) = WriteRequest::new(kvs);

        self.queue
            .send(request)
            .expect("Failed to send KVOpertion to queue");

        r
    }
}
pub struct DBReader<T: Store> {
    snapshot: LsmStorage<T>,
    id: u64, // Represents the max_op_id at the time the reader was created
}
impl<T: Store> DBReader<T> {
    pub fn set_id(&mut self, id: u64) {
        self.id = id;
    }
    pub fn query(&self, key: KeyVec) -> Option<KVOpertion> {
        // Create a KeyQuery using the reader's snapshot op_id.
        let query = common::KeyQuery {
            key: key.as_ref().into(), // Convert KeyVec to KeyBytes
            op_id: self.id,           // Use the snapshot OpId from the reader
        };
        // self.current is an Arc<LsmStorage<T>> holding the cloned state
        self.snapshot.get(&query)
    }
}
#[cfg(test)]
mod test {
    use super::common::OpId;
    use super::{Config, DBWriter, KeyBytes, OpType, ValueByte, WriteRequest};
    use crate::db::db_log;
    use crate::db::{store::Memstore, LsmDB};
    use crossbeam_channel::Receiver;
    use std::mem::size_of;
    use std::sync::atomic::Ordering;
    use tracing::{info, Level};
    use tracing_subscriber::filter::Targets;

    use crate::db::block::test::pad_zero;
    use crate::db::key::{KeyVec, ValueVec};
    use core::panic;
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        thread,
        time::{Duration, Instant}, // Add Instant and Duration for timing
    };

    // Helper function to write KVs and wait for compaction
    fn write_kvs_and_wait_for_compact(
        db: &mut LsmDB<Memstore>,
        config: Config,
        start_key: u64,
        num_kvs: usize,
    ) -> HashMap<KeyBytes, ValueByte> {
        let mut writer = db.get_writer();
        let (r, expected_data) = write_kvs(&mut writer, start_key, num_kvs);
        let _ = r.recv();
        wait_for_compact(db, config);
        expected_data
    }

    #[test]
    fn test_multiple_thread_write_and_read_in_orderd_batch() {
        // Setup: Create DB with default test configuration
        let mut config = crate::db::Config::config_for_test();
        config.lsm_storage_config.memtable_size_limit = 40000;
        let db = Arc::new(LsmDB::<Memstore>::new_with_memstore(config));

        let num_writer_threads = 3;
        let kvs_per_thread = 3000;
        let num_batches_per_thread = 3;
        let kvs_per_batch = kvs_per_thread / num_batches_per_thread;
        let total_kvs = num_writer_threads * kvs_per_thread;

        let expected_data: Arc<Mutex<HashMap<KeyBytes, ValueByte>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let mut handles = vec![];

        for thread_idx in 0..num_writer_threads {
            let db_clone = db.clone();
            let expected_data_clone = expected_data.clone();

            let handle = thread::spawn(move || {
                let mut writer = db_clone.get_writer();
                let start_key_idx = thread_idx * kvs_per_thread;

                for batch_idx in 0..num_batches_per_thread {
                    let mut kvs_to_write_batch: Vec<(KeyBytes, OpType)> =
                        Vec::with_capacity(kvs_per_batch);
                    let mut batch_expected_data: HashMap<KeyBytes, ValueByte> = HashMap::new();

                    for i in 0..kvs_per_batch {
                        let current_kv_idx = start_key_idx + batch_idx * kvs_per_batch + i;
                        let key_str = pad_zero(current_kv_idx as u64);
                        let value_str = format!("value_t{}_b{}_k{}", thread_idx, batch_idx, i);
                        let key_bytes = KeyBytes::from(key_str.as_bytes());
                        let value_bytes = ValueByte::from(value_str.as_bytes());

                        kvs_to_write_batch
                            .push((key_bytes.clone(), OpType::Write(value_bytes.clone())));
                        batch_expected_data.insert(key_bytes, value_bytes);
                    }

                    // Write batch
                    let r = writer.put(kvs_to_write_batch);
                    r.recv().expect(&format!(
                        "Write batch failed for thread {} batch {}",
                        thread_idx, batch_idx
                    ));

                    // Update global expected data
                    expected_data_clone
                        .lock()
                        .unwrap()
                        .extend(batch_expected_data.clone());

                    // Check by read after each write batch
                    let reader_after_batch = db_clone.get_reader();
                    for (key, expected_val) in batch_expected_data.iter() {
                        let result = reader_after_batch.query(KeyVec::from(key.clone().as_ref()));
                        match result {
                            Some(kv_op) => {
                                if let OpType::Write(actual_value) = kv_op.op {
                                    assert_eq!(
                                        actual_value.as_ref(),
                                        expected_val.as_ref(),
                                        "Value mismatch for key {:?} after batch write (thread {}, batch {})",
                                        key, thread_idx, batch_idx
                                    );
                                } else {
                                    panic!("Expected Write op, got {:?}", kv_op.op);
                                }
                            }
                            None => panic!(
                                "Key {:?} not found after batch write (thread {}, batch {})",
                                key, thread_idx, batch_idx
                            ),
                        }
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all writer threads to finish
        for handle in handles {
            handle.join().expect("Writer thread panicked");
        }

        // Wait until compaction finishes (give it ample time)
        // The SLEEP_TIME is 100ms, so 10 * 100ms = 1 second should be enough for a few cycles.
        // For larger data sets or slower machines, this might need adjustment.
        std::thread::sleep(std::time::Duration::from_millis(super::SLEEP_TIME * 10));

        // Check all data by read from a fresh reader
        let final_reader = db.get_reader();
        let expected_map = expected_data.lock().unwrap();
        assert!(
            final_reader
                .snapshot
                .table_num_in_levels()
                .iter()
                .sum::<usize>()
                > 0,
            "Tables should exist in levels"
        );

        let mut ves: Vec<KeyBytes> = vec![];

        let mut count = 00;
        for (key, expected_val) in expected_map.iter() {
            count += 1;
            let result = final_reader.query(KeyVec::from(key.clone().as_ref()));
            match result {
                Some(kv_op) => {
                    if let OpType::Write(actual_value) = kv_op.op {
                        assert_eq!(
                            actual_value.as_ref(),
                            expected_val.as_ref(),
                            "Final check: Value mismatch for key {:?}",
                            key
                        );
                    } else {
                        panic!("Final check: Expected Write op, got {:?}", kv_op.op);
                    }
                }
                None => panic!("Final check: Key {:?} not found", key),
                //     None => {
                //         ves.push(key);
                //     }
            }
        }
        ves.sort();
        if ves.len() > 0 {
            panic!("error")
        }
        assert_eq!(
            expected_map.len(),
            total_kvs as usize,
            "Total number of keys in expected map should match total KVs written"
        );
    }

    #[test]
    fn test_write_request_get_size() {
        let mut data = Vec::new();

        // Case 1: Write operation
        let key1 = KeyBytes::from("key1".as_bytes());
        let value1 = ValueByte::from("value1".as_bytes());
        data.push((key1.clone(), OpType::Write(value1.clone())));
        // Expected size for (key1, value1):
        // key_len (u64) + key_data + op_type (u8) + value_len (u64) + value_data
        // 8 + 4 + 1 + 8 + 6 = 27

        // Case 2: Delete operation
        let key2 = KeyBytes::from("key2".as_bytes());
        data.push((key2.clone(), OpType::Delete));
        // Expected size for (key2, Delete):
        // key_len (u64) + key_data + op_type (u8)
        // 8 + 4 + 1 = 13

        let (request, _) = WriteRequest::new(data);
        let actual_size = request.get_size();

        // Calculate expected size manually
        let expected_size_key1_write =
            size_of::<u64>() + key1.len() + size_of::<u8>() + size_of::<u64>() + value1.len();
        let expected_size_key2_delete = size_of::<u64>() + key2.len() + size_of::<u8>();

        let total_expected_size = expected_size_key1_write + expected_size_key2_delete;

        assert_eq!(
            actual_size, total_expected_size,
            "WriteRequest size mismatch"
        );
    }

    #[test]
    fn test_delete() {
        // Setup: Create DB with default test configuration
        let config = crate::db::Config::config_for_test();
        let db = LsmDB::<Memstore>::new_with_memstore(config);
        let mut writer = db.get_writer();

        let key_str = "test_key_to_delete";
        let value_str = "test_value";
        let key_bytes = KeyBytes::from(key_str.as_bytes());
        let value_bytes = ValueByte::from(value_str.as_bytes());

        // 1. Write the key-value pair
        let kvs_to_write = vec![(key_bytes.clone(), OpType::Write(value_bytes.clone()))];
        let r1 = writer.put(kvs_to_write);
        r1.recv().expect("Write failed to complete");

        // Verify it's found after write
        let reader_after_write = db.get_reader();
        let query_key_vec = KeyVec::from(key_str.as_bytes());
        let result_after_write = reader_after_write.query(query_key_vec.clone());
        assert!(
            result_after_write.is_some(),
            "Key should be found immediately after write"
        );
        if let Some(kv_op) = result_after_write {
            if let OpType::Write(actual_value) = kv_op.op {
                assert_eq!(
                    actual_value.as_ref(),
                    value_bytes.as_ref(),
                    "Value mismatch after write"
                );
            } else {
                panic!("Expected Write op, got {:?}", kv_op.op);
            }
        }

        // 2. Delete the key
        let kvs_to_delete = vec![(key_bytes.clone(), OpType::Delete)];
        let r2 = writer.put(kvs_to_delete);
        r2.recv().expect("Delete failed to complete");

        // 3. Query for the key and assert it should not be found
        let reader_after_delete = db.get_reader();
        let result_after_delete = reader_after_delete.query(query_key_vec.clone());
        assert!(
            result_after_delete.is_none(),
            "Key should not be found after deletion"
        );
    }
    #[test]
    fn test_snapshot_read() {
        // Setup: Create DB with default test configuration
        let config = crate::db::Config::config_for_test();
        let db = LsmDB::<Memstore>::new_with_memstore(config);
        let mut writer = db.get_writer();

        // 1. Write first batch of KVs
        let num_kvs_first_batch = 50;
        let mut kvs_to_write_first: Vec<(KeyBytes, OpType)> =
            Vec::with_capacity(num_kvs_first_batch);
        let mut expected_data_first: HashMap<KeyBytes, ValueByte> = HashMap::new();

        for i in 0..num_kvs_first_batch {
            let key_str = pad_zero(i as u64);
            let value_str = format!("value_first_{}", i);
            let key_bytes = KeyBytes::from(key_str.as_bytes());
            let value_bytes = ValueByte::from(value_str.as_bytes());
            kvs_to_write_first.push((key_bytes.clone(), OpType::Write(value_bytes.clone())));
            expected_data_first.insert(key_bytes, value_bytes);
        }
        let r1 = writer.put(kvs_to_write_first);
        r1.recv().expect("First write failed to complete");

        // 2. Get current op id as old_id
        let old_id = db.current_op_id();

        // 3. Write second batch of KVs
        let num_kvs_second_batch = 50;
        let mut kvs_to_write_second: Vec<(KeyBytes, OpType)> =
            Vec::with_capacity(num_kvs_second_batch);
        let mut expected_data_second: HashMap<KeyBytes, ValueByte> = HashMap::new();

        for i in num_kvs_first_batch..(num_kvs_first_batch + num_kvs_second_batch) {
            let key_str = pad_zero(i as u64);
            let value_str = format!("value_second_{}", i);
            let key_bytes = KeyBytes::from(key_str.as_bytes());
            let value_bytes = ValueByte::from(value_str.as_bytes());
            kvs_to_write_second.push((key_bytes.clone(), OpType::Write(value_bytes.clone())));
            expected_data_second.insert(key_bytes, value_bytes);
        }
        let r2 = writer.put(kvs_to_write_second);
        r2.recv().expect("Second write failed to complete");

        // 4. Use old_id to query: only old kvs can be found
        let mut reader = db.get_reader();
        reader.set_id(old_id - 1); // Set the reader's snapshot ID as last written id

        // Verify first batch keys (should be found)
        for i in 0..num_kvs_first_batch {
            let key_str = pad_zero(i as u64);
            let query_key_vec = KeyVec::from(key_str.as_bytes());
            let result = reader.query(query_key_vec.clone());

            let query_key_bytes = KeyBytes::from(key_str.as_bytes());
            let expected_value = expected_data_first.get(&query_key_bytes);

            match (result, expected_value) {
                (Some(kv_op), Some(expected_val)) => {
                    if let OpType::Write(actual_value) = kv_op.op {
                        assert_eq!(
                            actual_value.as_ref(),
                            expected_val.inner().as_ref(),
                            "Value mismatch for key {} in first batch",
                            key_str
                        );
                    } else {
                        panic!(
                            "Expected Write op for key {} in first batch, got {:?}",
                            key_str, kv_op.op
                        );
                    }
                }
                (None, Some(_)) => panic!(
                    "Key {} from first batch not found by snapshot reader",
                    key_str
                ),
                _ => panic!("Unexpected result for key {} from first batch", key_str),
            }
        }

        // Verify second batch keys (should NOT be found)
        for i in num_kvs_first_batch..(num_kvs_first_batch + num_kvs_second_batch) {
            let key_str = pad_zero(i as u64);
            let query_key_vec = KeyVec::from(key_str.as_bytes());
            let result = reader.query(query_key_vec.clone());

            assert!(
                result.is_none(),
                "Key {} from second batch unexpectedly found by snapshot reader (id: {})",
                key_str,
                reader.id
            );
        }
    }
    #[test]
    fn test_compact() {
        // Setup: Create DB with small memtable limit to trigger compaction
        let mut config = crate::db::Config::config_for_test();
        config.lsm_storage_config.memtable_size_limit = 1024; // Small limit
        config.imm_size_limit = 1; // Allow only one immutable memtable before dump
        let mut db = LsmDB::<Memstore>::new_with_memstore(config);

        // Write Data: Write enough data to exceed the memtable limit
        let mut writer = db.get_writer();
        let num_kvs = 50; // Enough KVs to likely exceed 1024 bytes
        let mut kvs_to_write: Vec<(KeyBytes, OpType)> = Vec::with_capacity(num_kvs);
        let mut expected_data: HashMap<KeyBytes, ValueByte> = HashMap::with_capacity(num_kvs);

        for i in 0..num_kvs {
            let key_str = pad_zero(i as u64);
            let value_str = format!("value_compact_{}", i); // Use distinct values
            let key_bytes = KeyBytes::from(key_str.as_bytes());
            let value_bytes = ValueByte::from(value_str.as_bytes());
            kvs_to_write.push((key_bytes.clone(), OpType::Write(value_bytes.clone())));
            expected_data.insert(key_bytes, value_bytes);
        }
        let r = writer.put(kvs_to_write);
        r.recv().expect("Write failed to complete"); // Wait for write to finish

        // Initial Check: Ensure no tables exist in levels yet (data in memtable)
        // Note: Depending on timing, the memtable might already be frozen.
        // A more robust check might be needed if the write worker is very fast.
        // For simplicity, we assume the compaction thread hasn't run yet.
        assert_eq!(
            db.tables_count(),
            0,
            "Initially, there should be no tables in levels"
        );

        // start compact
        db.trigger_compact();
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Check Levels: Ensure tables were created by compaction/dump
        assert!(
            db.tables_count() > 0,
            "After waiting, tables should exist in levels due to dump/compaction"
        );

        // Verify Data: Check some keys
        let reader = db.get_reader();
        for i in (0..num_kvs).step_by(5) {
            // Check a subset of keys
            let key_str = pad_zero(i as u64);
            let query_key_vec = KeyVec::from(key_str.as_bytes());
            let result = reader.query(query_key_vec.clone());

            let query_key_bytes = KeyBytes::from(key_str.as_bytes());
            let expected_value = expected_data.get(&query_key_bytes);

            match (result, expected_value) {
                (Some(kv_op), Some(expected_val)) => {
                    if let OpType::Write(actual_value) = kv_op.op {
                        assert_eq!(
                            actual_value.as_ref(),
                            expected_val.inner().as_ref(),
                            "Value mismatch for key {} after compaction",
                            key_str
                        );
                    } else {
                        panic!(
                            "Expected Write op for key {} after compaction, got {:?}",
                            key_str, kv_op.op
                        );
                    }
                }
                (None, Some(_)) => panic!("Key {} not found after compaction", key_str),
                (Some(_), None) => panic!("Unexpected key {} found after compaction", key_str),
                (None, None) => panic!("Key {} not found (and not expected?)", key_str), // Should not happen
            }
        }
    }

    fn wait_for_compact(db: &mut LsmDB<Memstore>, config: crate::db::Config) {
        let initial_compact_count = db.statistic.compact_count.load(Ordering::SeqCst);

        // Trigger compaction manually.
        db.trigger_compact();

        // Loop and wait for compact_count to increase by at least one.
        let mut loop_count = 0;
        loop {
            let current_compact_count = db.statistic.compact_count.load(Ordering::SeqCst);
            if current_compact_count > initial_compact_count {
                break;
            }
            loop_count += 1;
            if loop_count > 10 {
                panic!(
                    "wait_for_compact loop exceeded 10 iterations without compaction. \
                    Initial: {}, Current: {}",
                    initial_compact_count, current_compact_count
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(config.thread_sleep_time));
            // Prevent busy-waiting
        }
    }

    /// Helper function to prepare KV operations and send them to the writer.
    fn write_kvs(
        writer: &mut DBWriter,
        start_key: u64,
        num_kvs: usize,
    ) -> (Receiver<OpId>, HashMap<KeyBytes, ValueByte>) {
        let mut kvs_to_write: Vec<(KeyBytes, OpType)> = Vec::with_capacity(num_kvs);
        let mut expected_data: HashMap<KeyBytes, ValueByte> = HashMap::with_capacity(num_kvs);

        for i in 0..num_kvs {
            let current_key_idx = start_key + i as u64;
            let key_str = pad_zero(current_key_idx);
            let value_str = format!("{}{}", "value_", current_key_idx);
            let key_bytes = KeyBytes::from(key_str.as_bytes());
            let value_bytes = ValueByte::from(value_str.as_bytes());
            kvs_to_write.push((key_bytes.clone(), OpType::Write(value_bytes.clone())));
            expected_data.insert(key_bytes, value_bytes);
        }

        let r = writer.put(kvs_to_write);
        (r, expected_data)
    }

    /// Tests the backpressure mechanism where writes are blocked if too many immutable memtables are pending.
    /// It verifies that writes resume after compaction clears the pending memtables.
    #[test]
    fn test_backpressure() {
        // Setup: Config with small memtable limit, imm_size_limit = 2, auto_compact = false
        let mut config = crate::db::Config::config_for_test();
        config.lsm_storage_config.memtable_size_limit = 1000; // Small limit for testing
        config.imm_size_limit = 2; // Allow only one immutable memtable before backpressure
        config.auto_compact = false; // Disable auto-compaction for manual control
        config.thread_sleep_time = 10; // Shorter sleep time for faster test feedback

        let mut db = LsmDB::<Memstore>::new_with_memstore(config);
        let mut writer = db.get_writer();

        let kvs_per_batch = 50; // Each KV is roughly 20 bytes (key+value+metadata), 50 KVs = 1KB
        let mut total_expected_data: HashMap<KeyBytes, ValueByte> = HashMap::new();

        // 1. Write first batch: Fills memtable and freezes it. imm_num = 1.
        info!("Test backpressure: Writing first batch");
        let mut w = db.get_writer();
        let (r, kvs1_data) = write_kvs(&mut w, 0, kvs_per_batch);
        total_expected_data.extend(kvs1_data);
        // Add a small sleep to allow the writer thread to process the request and freeze memtable
        std::thread::sleep(std::time::Duration::from_millis(
            config.thread_sleep_time * 2,
        ));
        // After this, one immutable memtable should exist.
        assert_eq!(
            db.get_reader().snapshot.immtable_num(),
            1,
            "Expected 1 immutable memtable after first write"
        );
        assert_eq!(
            db.check_memtable_size(),
            0,
            "Active memtable should be empty after freeze"
        );

        // 2. Write second batch: Fills new memtable and freezes it. imm_num becomes 2.
        // This will cause the write worker to enter backpressure sleep.
        info!("Test backpressure: Writing second batch, expecting backpressure");
        let mut kvs2_to_write: Vec<(KeyBytes, OpType)> = Vec::with_capacity(kvs_per_batch);
        for i in kvs_per_batch..(kvs_per_batch * 2) {
            let key_str = pad_zero(i as u64);
            let value_str = format!("value_backpressure_{}", i);
            let key_bytes = KeyBytes::from(key_str.as_bytes());
            let value_bytes = ValueByte::from(value_str.as_bytes());
            kvs2_to_write.push((key_bytes.clone(), OpType::Write(value_bytes.clone())));
            total_expected_data.insert(key_bytes, value_bytes);
        }
        let r2 = writer.put(kvs2_to_write);
        // Do not wait for r2.recv() here, as it's expected to block the worker.
        // The worker thread will process it and then block when it tries to freeze
        // a *third* memtable and immtable_num() is already 2 >= imm_size_limit (1).

        // Give the writer a moment to process the second batch and hit backpressure
        std::thread::sleep(std::time::Duration::from_millis(
            config.thread_sleep_time * 2,
        ));
        assert_eq!(
            db.get_reader().snapshot.immtable_num(),
            2,
            "Expected 2 immutable memtables after second write"
        );

        // 3. Initiate a third write, which should immediately block the worker
        // if the previous writes pushed it into backpressure.
        info!("Test backpressure: Initiating third write, expecting it to block.");
        let mut kvs3_to_write: Vec<(KeyBytes, OpType)> = Vec::with_capacity(1);
        let key_str = pad_zero((kvs_per_batch * 2) as u64);
        let value_str = format!("value_backpressure_{}", kvs_per_batch * 2);
        let key_bytes = KeyBytes::from(key_str.as_bytes());
        let value_bytes = ValueByte::from(value_str.as_bytes());
        kvs3_to_write.push((key_bytes.clone(), OpType::Write(value_bytes.clone())));
        total_expected_data.insert(key_bytes, value_bytes);

        let r3 = writer.put(kvs3_to_write);

        // Verify the third write is blocked (or the write worker is in backpressure loop)
        let blocked_timeout = std::time::Duration::from_millis(config.thread_sleep_time * 5);
        let start_time = Instant::now();
        let r3_result = r3.recv_timeout(blocked_timeout);
        assert!(
            r3_result.is_err(),
            "Third write should be blocked due to backpressure, but it completed: {:?}",
            r3_result
        );
        assert!(
            start_time.elapsed() >= blocked_timeout,
            "Third write was not blocked for the expected duration."
        );
        info!("Third write is blocked as expected.");

        // 4. Trigger manual compaction
        info!("Test backpressure: Triggering manual compaction.");
        wait_for_compact(&mut db, config);
        info!("Test backpressure: Compaction finished.");

        // Verify the previously blocked second and third writes complete
        r2.recv()
            .expect("Second write should complete after compaction");
        r3.recv()
            .expect("Third write should complete after compaction");
        info!("All pending writes completed after compaction.");

        // Check levels: should have tables
        assert!(
            db.tables_count() > 0,
            "After compaction, tables should exist in levels"
        );

        // Verify all data by read from a fresh reader
        info!("Test backpressure: Verifying all data.");
        let final_reader = db.get_reader();
        for (key, expected_val) in total_expected_data.iter() {
            let result = final_reader.query(KeyVec::from(key.clone().as_ref()));
            match result {
                Some(kv_op) => {
                    if let OpType::Write(actual_value) = kv_op.op {
                        assert_eq!(
                            actual_value.as_ref(),
                            expected_val.as_ref(),
                            "Final check: Value mismatch for key {:?}",
                            key
                        );
                    } else {
                        panic!("Final check: Expected Write op, got {:?}", kv_op.op);
                    }
                }
                None => panic!("Final check: Key {:?} not found", key),
            }
        }
        info!("Test backpressure: All data verified successfully.");
    }

    #[test]
    fn test_write_request_opid_return() {
        let config = crate::db::Config::config_for_test();
        let db = LsmDB::<Memstore>::new_with_memstore(config);
        let mut writer = db.get_writer();

        let initial_op_id = db.current_op_id();

        // Prepare a batch of KVs
        let num_kvs_in_batch = 5;
        let mut kvs_to_write: Vec<(KeyBytes, OpType)> = Vec::with_capacity(num_kvs_in_batch);
        for i in 0..num_kvs_in_batch {
            let key_str = format!("key_opid_test_{}", i);
            let value_str = format!("value_opid_test_{}", i);
            let key_bytes = KeyBytes::from(key_str.as_bytes());
            let value_bytes = ValueByte::from(value_str.as_bytes());
            kvs_to_write.push((key_bytes, OpType::Write(value_bytes)));
        }

        // Send the write request and wait for the OpId
        let r = writer.put(kvs_to_write);
        let received_op_id = r.recv().expect("Failed to receive OpId from write request");

        // The OpId returned should be the last OpId of the batch.
        // Since current_op_id is incremented *after* the batch,
        // (current_op_id - 1) is the last OpId of the previous batch.
        // So, initial_op_id + num_kvs_in_batch - 1 should be the expected OpId.
        let expected_op_id = initial_op_id + num_kvs_in_batch as u64 - 1;

        assert_eq!(
            received_op_id, expected_op_id,
            "Received OpId mismatch with expected last OpId of the batch"
        );

        // Verify the db's current_op_id is correctly updated
        assert_eq!(
            db.current_op_id(),
            initial_op_id + num_kvs_in_batch as u64,
            "DB's current_op_id not updated correctly"
        );

        info!("test_write_request_opid_return completed successfully.");
    }

    // write/delete date use unordered key
    // use const seed
    // create 2000 kvs and shuffle it
    // write them to db and then random delete 500
    // write/delete date use unordered key
    // create 2000 kvs and shuffle it
    // write them to db and then random delete 500
    // check it by get
    #[test]
    fn test_random_write_delete() {
        let mut config = crate::db::Config::config_for_test();
        config.lsm_storage_config.memtable_size_limit = 40000;
        config.auto_compact = true; // Let auto-compact run in background
        config.thread_sleep_time = 10;
        let mut db = LsmDB::<Memstore>::new_with_memstore(config);
        let mut writer = db.get_writer();

        let total_kvs = 2000;
        let num_to_delete = 500;

        let mut all_kvs: Vec<(KeyBytes, OpType)> = Vec::with_capacity(total_kvs);
        let mut expected_data_after_write: HashMap<KeyBytes, ValueByte> =
            HashMap::with_capacity(total_kvs);

        // 1. Generate and write initial KVs
        for i in 0..total_kvs {
            let key_str = pad_zero(i as u64);
            let value_str = format!("value_{}", i);
            let key_bytes = KeyBytes::from(key_str.as_bytes());
            let value_bytes = ValueByte::from(value_str.as_bytes());
            all_kvs.push((key_bytes.clone(), OpType::Write(value_bytes.clone())));
            expected_data_after_write.insert(key_bytes, value_bytes);
        }

        // Shuffle all_kvs for unordered writes
        use rand::rngs::StdRng;
        use rand::seq::SliceRandom;
        use rand::SeedableRng; // Add this import

        // Use a fixed seed for reproducibility
        let mut rng = StdRng::seed_from_u64(12345);
        all_kvs.shuffle(&mut rng);

        info!("Writing {} random KVs in batches of 100...", total_kvs);
        let batch_size = 100;
        for chunk in all_kvs.chunks(batch_size) {
            let r_write_batch = writer.put(chunk.to_vec());
            r_write_batch.recv().expect("Random write batch failed");
        }
        info!("Initial writes completed.");

        // 2. Select keys to delete and perform deletes
        let mut keys_to_delete: Vec<KeyBytes> = Vec::with_capacity(num_to_delete);
        let mut all_keys: Vec<KeyBytes> = expected_data_after_write.keys().cloned().collect();
        all_keys.shuffle(&mut rng);

        for i in 0..num_to_delete {
            let key = all_keys.pop().unwrap(); // Get a random key to delete
            keys_to_delete.push(key.clone());
            expected_data_after_write.remove(&key); // Remove from expected map
        }

        info!("Deleting {} random KVs in batches of 10...", num_to_delete);
        let delete_batch_size = 10;
        for chunk in keys_to_delete.chunks(delete_batch_size) {
            let mut delete_ops: Vec<(KeyBytes, OpType)> = Vec::with_capacity(chunk.len());
            for key in chunk.iter() {
                delete_ops.push((key.clone(), OpType::Delete));
            }
            let r_delete_batch = writer.put(delete_ops);
            r_delete_batch.recv().expect("Random delete batch failed");
        }
        info!("Delete operations completed.");

        info!("Waiting for all background operations to complete...");
        let mut loop_count = 0;
        loop {
            if !db.need_freeze() && !db.need_dump() && !db.need_compact() {
                break;
            }
            loop_count += 1;
            if loop_count > 50 {
                // Add a timeout to prevent infinite loops in case of test issues
                panic!("Timeout waiting for background operations to complete.");
            }
            std::thread::sleep(std::time::Duration::from_millis(config.thread_sleep_time));
        }
        info!("All background operations completed.");

        // 3. Verify data consistency
        info!("Verifying data consistency after writes and deletes.");
        let reader = db.get_reader();

        // Check keys that should still exist
        for (key, expected_val) in expected_data_after_write.iter() {
            let result = reader.query(KeyVec::from(key.as_ref()));
            match result {
                Some(kv_op) => {
                    if let OpType::Write(actual_value) = kv_op.op {
                        assert_eq!(
                            actual_value.as_ref(),
                            expected_val.as_ref(),
                            "Remaining key {:?} value mismatch",
                            key
                        );
                    } else {
                        panic!(
                            "Expected Write op for remaining key {:?}, got {:?}",
                            key, kv_op.op
                        );
                    }
                }
                None => panic!("Remaining key {:?} not found", key),
            }
        }
        info!(
            "Verified {} remaining keys.",
            expected_data_after_write.len()
        );

        // Check keys that should have been deleted
        for key in keys_to_delete.iter() {
            let result = reader.query(KeyVec::from(key.as_ref()));
            assert!(result.is_none(), "Deleted key {:?} found unexpectedly", key);
        }
        info!("Verified {} deleted keys are absent.", num_to_delete);

        // Assert total count matches
        assert_eq!(
            db.tables_count(),
            db.tables_count(),
            "Table count in levels mismatch with reported total"
        );
        assert_eq!(
            expected_data_after_write.len(),
            total_kvs - num_to_delete,
            "Final expected data map size mismatch"
        );
        info!("Random write/delete test completed successfully.");
    }

    /// Tests basic put and query functionality.
    /// It writes a large number of key-value pairs, waits for compaction,
    /// and then queries all keys to ensure data integrity and retrieval.
    #[test]
    fn test_basic_put_and_query() {
        // create lsmdb use memstore 2 level ratio  2 zero level file
        // The Config::config_for_test() sets memtable_size_limit and imm_size_limit.
        let mut config = Config::config_for_test();
        config.auto_compact = false;
        config
            .lsm_storage_config
            .level_config
            .table_config
            .set_table_size(10 * 1024);
        let mut db = LsmDB::<Memstore>::new_with_memstore(config);

        let num_kvs = 1000;
        let mut expected_data = write_kvs_and_wait_for_compact(&mut db, config, 0, num_kvs);

        assert_eq!(
            db.table_num_in_levels().len(),
            2,
            "DB should have 2 levels after compaction and dump"
        );

        // Write more data after the first compaction to ensure memtable and immutable memtables are used during reads
        info!("write more data after compact");
        let num_kvs_second_batch = 30;
        // write to immutable
        let (r_second, expected_data_second_batch) =
            write_kvs(&mut db.get_writer(), num_kvs as u64, num_kvs_second_batch);
        r_second
            .recv()
            .expect("Second write batch failed to complete");
        // write to memtable
        let num_kvs_third_batch = 1;
        let (r_third, expected_data_second_batch) =
            write_kvs(&mut db.get_writer(), num_kvs as u64, num_kvs_third_batch);
        r_third
            .recv()
            .expect("Second write batch failed to complete");
        expected_data.extend(expected_data_second_batch);
        assert!(
            db.memtable_size() > 0,
            "Active memtable should not be empty after second batch write"
        );
        assert!(
            db.imm_memtable_count() > 0,
            "Immutable memtables should exist after second batch write"
        );

        let reader = db.get_reader(); // Snapshot reader. This reader will see the newly written data.

        let start_time = Instant::now(); // Record start time for read loop
        for i in 0..num_kvs {
            let key_str = pad_zero(i as u64);
            let query_key_vec = KeyVec::from(key_str.as_bytes());
            let result = reader.query(query_key_vec.clone()); // query expects KeyVec

            // HashMap uses KeyBytes, so convert for lookup
            let query_key_bytes = KeyBytes::from(key_str.as_bytes());
            let expected_value = expected_data.get(&query_key_bytes);

            match (result, expected_value) {
                (Some(kv_op), Some(expected_val)) => {
                    if let OpType::Write(actual_value) = kv_op.op {
                        assert_eq!(
                            actual_value.as_ref(),
                            expected_val.inner().as_ref(), // Access inner Bytes and then as_ref()
                            "Value mismatch for key {}",
                            key_str
                        );
                    } else {
                        panic!(
                            "Expected a Write operation for key {}, but got {:?}",
                            key_str, kv_op.op
                        );
                    }
                }
                (None, Some(_)) => {
                    panic!("Key {} was expected to be found but was not.", key_str);
                }
                (Some(_), None) => {
                    panic!("Key {} was not expected to be found but was.", key_str);
                }
                (None, None) => {
                    // This case should not happen for initial writes.
                    panic!(
                        "Key {} was not found, but was expected to be present.",
                        key_str
                    );
                }
            }
        }
    }
}
