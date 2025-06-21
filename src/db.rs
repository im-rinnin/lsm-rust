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
use once_cell::sync::Lazy;
use store::{fetch_add_store_id, Memstore, Store, StoreId};
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
    writer_handle: Option<JoinHandle<()>>,
    compaction_handle: Option<JoinHandle<()>>,
    trigger_compact: Sender<()>,
    statistic: Arc<DBStatistic>,
    config: Config,
}
const SLEEP_TIME: u64 = 10;
#[derive(Clone, Copy)]
pub struct Config {
    pub lsm_storage_config: LsmStorageConfig,
    pub request_queue_len: usize,
    pub imm_num_limit: usize,
    pub thread_sleep_time: u64,
    pub request_queue_size: usize,
    pub auto_compact: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            lsm_storage_config: LsmStorageConfig::default(),
            request_queue_len: 500,
            imm_num_limit: 2,
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

        // Wait for worker threads to finish
        // need to stop writer first to freeze memtable and then dump imm memtable
        let writer_join_result = self.writer_handle.take().unwrap().join();
        if let Err(e) = writer_join_result {
            info!("Writer thread panicked: {:?}", e);
        }
        let compaction_join_result = self.compaction_handle.take().unwrap().join();
        if let Err(e) = compaction_join_result {
            info!("Compaction thread panicked: {:?}", e);
        }
        info!("close db end drop");
    }
}

impl LsmDB<Memstore> {
    pub fn new_with_memstore(config: Config) -> Self {
        // Create the initial LsmStorage, wrapped in Arc<RwLock<...>>
        let initial_lsm_storage = Arc::new(RwLock::new(LsmStorage::new(config.lsm_storage_config)));

        let mut store_id = fetch_add_store_id();
        // Create LogFile and TableChangeLog with unique Memstore instances
        let logfile_store_id = store_id;
        store_id += 1;
        let logfile = LogFile::<Memstore>::open(logfile_store_id);

        let level_meta_log_store_id = store_id; // Use a different ID from logfile
        store_id += 1;
        let level_meta_log = TableChangeLog::<Memstore>::from(level_meta_log_store_id);

        // Initialize atomic counters
        let current_max_op_id = Arc::new(AtomicU64::new(0));

        // Create the stop flag for worker threads
        let stop_flag = Arc::new(Mutex::new(false));

        // Create the channel for write requests
        let (s, r) = crossbeam_channel::bounded(config.request_queue_len);
        let writer_queue_r = r.clone(); // Clone for writer thread

        // Initialize atomic counters
        let current_max_op_id = Arc::new(AtomicU64::new(0));
        let max_op_for_writer = current_max_op_id.clone();

        // Initialize DBStatistic once
        let statistic = Arc::new(DBStatistic::default());
        let statistic_for_compactor = statistic.clone();

        // Prepare LsmStorage Arc for cloning to threads
        let initial_lsm_storage_for_writer = initial_lsm_storage.clone();
        let initial_lsm_storage_for_compactor = initial_lsm_storage.clone();

        // Clone stop flag for threads
        let w_stop_flag = stop_flag.clone();
        let c_stop_flag = stop_flag.clone();

        // config is Copy, so we can pass it by value or clone for threads
        let writer_config = config;
        let compaction_config = config;

        // Determine next_sstable_id_start_value
        let mut next_sstable_id_start_value: StoreId = store_id;
        store_id += 1; // Increment for compaction thread's starting ID

        // Create the channel for compaction triggers
        let (trigger_s, trigger_r) = unbounded();

        // Spawn writer thread
        let writer_handle = Some(thread::spawn(move || {
            LsmDB::write_worker(
                w_stop_flag,
                writer_queue_r,
                initial_lsm_storage_for_writer,
                logfile, // logfile is moved here
                max_op_for_writer,
                writer_config,
            );
        }));

        // Spawn compaction thread
        let compaction_handle = Some(thread::spawn(move || {
            LsmDB::dump_and_compact_thread(
                initial_lsm_storage_for_compactor,
                level_meta_log, // level_meta_log is moved here
                next_sstable_id_start_value,
                c_stop_flag,
                compaction_config,
                trigger_r,
                statistic_for_compactor,
            );
        }));

        LsmDB {
            current_max_op_id,
            current: initial_lsm_storage,
            request_queue_r: r,
            request_queue_s: s,
            stop_flag, // Ownership moved here
            writer_handle,
            compaction_handle,
            trigger_compact: trigger_s,
            statistic,
            config,
        }
    }

    pub fn trigger_compact(&mut self) {
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
                return; // Exit the inner loop and then the outer loop
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

            let mut lsm_storage_clone = current.read().unwrap().clone();
            // Dump all immutable memtables until level zero reach limit
            while lsm_storage_clone.immtable_num() > 0
                && !lsm_storage_clone.level_zero_reach_limit()
            {
                info!(
                    immtable_num = lsm_storage_clone.immtable_num(),
                    "dump memtable"
                );
                lsm_storage_clone.dump_imm_memtable(&mut next_sstable_id);
            }
            *current.write().unwrap() = lsm_storage_clone;
            // Perform compaction and loop immediately if more compaction is needed
            loop {
                if Self::check_compact_thread_stop_signal(&stop) {
                    return; // Exit the inner loop and then the outer loop
                }
                let mut lsm_storage_clone = current.read().unwrap().clone();

                let need_compact = lsm_storage_clone.need_compact();
                if !need_compact {
                    info!("dont need compact return");
                    break;
                }
                let table_changes = lsm_storage_clone.compact_level(&mut next_sstable_id);
                if table_changes.len() > 0 {
                    meta.append(table_changes); // Persist changes to the TableChangeLog
                }

                info!("after a compact cycle");
                lsm_storage_clone.log_lsm_debug_info();

                // Update current LsmStorage state
                *current.write().unwrap() = lsm_storage_clone;
                statistic.compact_count.fetch_add(1, Ordering::SeqCst);
                info!("compact finish");
            }
        }
        // dump all remaining immutable memtables at the end
        {
            info!("dump remaining imm memtable at end of compaction thread.");
            let mut lsm_storage_clone = current.write().unwrap();
            if lsm_storage_clone.immtable_num() > 0 {
                info!("dump remaining imm memtable at end of compaction thread.");
                lsm_storage_clone.dump_imm_memtable(&mut next_sstable_id);
            }
            info!("compaction thread exiting.");
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
                    while lsm_storage_guard.immtable_num() >= c.imm_num_limit {
                        drop(lsm_storage_guard); // Release write lock before sleeping
                        info!(
                            "Too many immutable memtables ({}), waiting for compaction...",
                            c.imm_num_limit
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
        // freeze the memtable if it is not empty
        {
            info!("Freezing memtable at end of write worker.");
            let mut lsm_storage_guard = current_lsm.write().unwrap();
            if lsm_storage_guard.memtable_size() > 0 {
                info!("Freezing memtable at end of write worker.");
                lsm_storage_guard.freeze_memtable();
            }
            info!("Write worker exiting.");
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
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::mem::size_of;
    use std::sync::atomic::Ordering;
    use std::sync::RwLock;
    use tracing::{info, warn, Level};
    use tracing_subscriber::filter::Targets;

    use crate::db::block::test::pad_zero;
    use crate::db::common::KVOpertion; // Added for test_mutiple_thread_random_operaiton
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
        config.imm_num_limit = 1; // Allow only one immutable memtable before dump
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
        // Setup: Create DB with small memtable limit and imm_size_limit, disable auto_compact
        let mut config = crate::db::Config::config_for_test();
        config.lsm_storage_config.memtable_size_limit = 1024; // Small memtable limit
        config.imm_num_limit = 1; // Allow only one immutable memtable before backpressure
        config.auto_compact = false; // Disable auto-compaction for controlled testing
        config.thread_sleep_time = 10; // Short sleep time for faster test execution

        let mut db = LsmDB::<Memstore>::new_with_memstore(config);
        let mut writer = db.get_writer();

        // 1. Write enough data to fill the memtable and create one immutable memtable.
        // This should trigger the `imm_size_limit` backpressure.
        info!("Writing first batch to fill memtable and create one immutable...");
        let num_kvs_first_batch = 100; // Should be enough to fill 1024 bytes and freeze
        let (r1, _expected_data1) = write_kvs(&mut writer, 0, num_kvs_first_batch);
        r1.recv().expect("First write batch failed to complete");

        // Wait for the write worker to freeze the memtable and create an immutable one.
        // It should now have 1 immutable memtable.
        let mut loop_count = 0;
        loop {
            if db.imm_memtable_count() >= config.imm_num_limit {
                break;
            }
            loop_count += 1;
            if loop_count > 100 {
                panic!("Timeout waiting for first immutable memtable to be created.");
            }
            std::thread::sleep(std::time::Duration::from_millis(config.thread_sleep_time));
        }
        info!(
            "First immutable memtable created. Count: {}",
            db.imm_memtable_count()
        );
        assert_eq!(db.imm_memtable_count(), 1);

        // 2. Write another batch. This should cause backpressure because imm_size_limit is 1.
        // The write worker should block until compaction reduces the immutable count.
        info!("Writing second batch, expecting backpressure...");
        let num_kvs_second_batch = 10; // Small batch, but should trigger backpressure
        let (r2, _expected_data2) = write_kvs(&mut writer, 1000, num_kvs_second_batch);

        // Check that the second write is NOT immediately completed.
        // We expect it to block for a duration longer than a typical non-blocked write.
        let start_time = Instant::now();
        let recv_result = r2.recv_timeout(Duration::from_millis(config.thread_sleep_time * 5)); // Wait for 5x sleep time
        assert!(
            recv_result.is_err(),
            "Second write completed too quickly, backpressure might not be active."
        );
        assert!(
            start_time.elapsed().as_millis() >= (config.thread_sleep_time * 5) as u128,
            "Second write did not block for expected duration."
        );
        info!("Second write is blocked as expected.");

        // 3. Manually trigger compaction.
        // This should cause the immutable memtable to be dumped to an SSTable,
        // reducing the immutable count and unblocking the write worker.
        info!("Triggering manual compaction...");
        db.trigger_compact();

        // 4. Wait for the second write to complete. It should now finish.
        let recv_result_after_compact = r2.recv();
        assert!(
            recv_result_after_compact.is_ok(),
            "Second write did not complete after compaction was triggered."
        );
        info!("Second write completed after compaction.");

        // Verify that the immutable memtable count is now lower (ideally 0 or 1 if a new one was frozen)
        // and that tables exist in levels.
        let mut loop_count = 0;
        loop {
            if db.imm_memtable_count() == 0 && db.tables_count() > 0 {
                break;
            }
            loop_count += 1;
            if loop_count > 100 {
                panic!("Timeout waiting for compaction to fully clear immutable memtables.");
            }
            std::thread::sleep(std::time::Duration::from_millis(config.thread_sleep_time));
        }
        info!(
            "Immutable memtable count after compaction: {}",
            db.imm_memtable_count()
        );
        assert_eq!(db.imm_memtable_count(), 0);
        assert!(
            db.tables_count() > 0,
            "Tables should have been created in levels."
        );

        info!("test_backpressure completed successfully.");
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

    //3 thread do write delete get in random order
    //
    //create db enable auto compact
    //create a thread safe hashmap call data_states, contains all data latest state map<arc<mutex(key,optype)>>
    //fill db with init data key from [0..1000],set them to hashmap data_states
    //start 3 thread random pick key in range 0..3000, pick random operaiotn (write/put/get),do it, compare result with data_states(if read),update data_states
    // wait for every thread do 1000 operaiotn
    #[test]
    fn test_mutiple_thread_random_operaiton() {
        let mut config = crate::db::Config::config_for_test();
        config.lsm_storage_config.memtable_size_limit = 10000;
        config.imm_num_limit = 10;
        config.auto_compact = true; // Let auto-compact run in background
        config.thread_sleep_time = 1;
        let mut db = LsmDB::<Memstore>::new_with_memstore(config);
        let mut writer = db.get_writer();

        // data_states: HashMap where each value is an Arc<Mutex<Option<KVOpertion>>>
        // The outer Mutex protects the HashMap itself (adding/removing keys).
        // The inner Mutex protects the KVOpertion for a specific key.
        let data_states: Arc<RwLock<HashMap<KeyBytes, Arc<Mutex<Option<KVOpertion>>>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // 1. Initial data population (keys 0..1000)
        let num_initial_kvs = 1000;
        let mut initial_kvs_to_write: Vec<(KeyBytes, OpType)> = Vec::with_capacity(num_initial_kvs);
        for i in 0..num_initial_kvs {
            let key_str = pad_zero(i as u64);
            let value_str = format!("initial_value_{}", i);
            let key_bytes = KeyBytes::from(key_str.as_bytes());
            let value_bytes = ValueByte::from(value_str.as_bytes());
            initial_kvs_to_write.push((key_bytes, OpType::Write(value_bytes)));
        }
        info!("Writing initial {} KVs to DB...", num_initial_kvs);
        let r_initial = writer.put(initial_kvs_to_write.clone());
        r_initial.recv().expect("Initial write failed to complete");

        // Populate data_states based on initial write
        {
            let mut data_states_guard = data_states.write().unwrap();
            let current_max_op_id_after_init = db.current_op_id();
            // The OpId for the first key in the batch is `current_max_op_id - batch_size`.
            let mut initial_op_id_base = current_max_op_id_after_init - num_initial_kvs as u64;

            for (key_bytes, op_type) in initial_kvs_to_write {
                let op_id = initial_op_id_base;
                initial_op_id_base += 1;
                let new_kv_op = KVOpertion::new(op_id, KeyVec::from(key_bytes.as_ref()), op_type);
                data_states_guard.insert(key_bytes, Arc::new(Mutex::new(Some(new_kv_op))));
            }
        }
        info!("Initial KVs populated in DB and data_states.");

        // 2. Multi-threaded random operations
        let num_threads = 20;
        let ops_per_thread = 1000;
        let total_random_keys = 3000; // Keys from 0 to 2999

        let mut handles = vec![];

        let db = Arc::new(db);
        for thread_idx in 0..num_threads {
            let db_clone = db.clone();
            let data_states_clone = data_states.clone();

            let handle = thread::spawn(move || {
                let mut writer = db_clone.get_writer();
                let mut rng = StdRng::seed_from_u64(thread_idx as u64 + 123); // Seed per thread for distinct sequences

                for ops_done_in_thread in 0..ops_per_thread {
                    let key_idx = rng.gen_range(0..total_random_keys);
                    let key_str = pad_zero(key_idx as u64);
                    let key_bytes = KeyBytes::from(key_str.as_bytes());

                    // 0=get, 1=write, 2=delete
                    let op_choice = rng.gen_range(0..3);

                    match op_choice {
                        0 => {
                            // GET operation
                            let data_states_guard = data_states_clone.read().unwrap(); // Read access to the HashMap
                            let key_entry_mutex_option = data_states_guard.get(&key_bytes);

                            if let Some(key_entry_mutex) = key_entry_mutex_option {
                                let data_state_entry_guard = key_entry_mutex.lock().unwrap();
                                let current_reader = db_clone.get_reader(); // Snapshot reader for this query
                                let db_result =
                                    current_reader.query(KeyVec::from(key_bytes.as_ref()));
                                if let Some(expected_kv) = &*data_state_entry_guard {
                                    if expected_kv.id <= current_reader.id {
                                        match (&expected_kv.op, db_result) {
                                            (OpType::Write(expected_val), Some(actual_kv)) => {
                                                if let OpType::Write(actual_val) = actual_kv.op {
                                                    assert_eq!(actual_val.as_ref(), expected_val.as_ref(),
                                                        "GET (visible): Value mismatch for key {:?}. Reader ID: {}, Expected OpId: {}",
                                                        key_bytes, current_reader.id, expected_kv.id);
                                                } else {
                                                    panic!("GET (visible): Expected write, but found non-write op {:?} for key {:?}",
                                                           actual_kv.op, key_bytes);
                                                }
                                            }
                                            (OpType::Delete, None) => { /* Correct */ }
                                            (OpType::Write(_), None) => {
                                                panic!("GET (visible): Key {:?} expected to be found (Write), but not found by reader (id {}). Expected OpId: {}",
                                                       key_bytes, current_reader.id, expected_kv.id);
                                            }
                                            (OpType::Delete, Some(actual_kv)) => {
                                                panic!("GET (visible): Key {:?} expected to be deleted (visible Delete), but found {:?}. Reader ID: {}",
                                                       key_bytes, actual_kv.op, current_reader.id);
                                            }
                                        }
                                    } else {
                                        // expected_kv.id > current_reader.id (not visible)
                                        if let Some(actual_kv) = db_result {
                                            if actual_kv.id >= expected_kv.id {
                                                panic!(
                                                    "GET (not visible): Key {:?} found with OpId {} >= latest expected OpId {} (not visible by reader id {})",
                                                    key_bytes, actual_kv.id, expected_kv.id, current_reader.id
                                                );
                                            }
                                        }
                                    }
                                } else {
                                    // `key_entry_mutex` holds `None`
                                    assert!(db_result.is_none(),
                                        "GET: Key {:?} not expected (data_states `None`) but found in DB: {:?}", key_bytes, db_result);
                                }
                            } else {
                                let current_reader = db_clone.get_reader(); // Snapshot reader for this query
                                let db_result =
                                    current_reader.query(KeyVec::from(key_bytes.as_ref()));
                                // Key not present in `data_states` HashMap
                                assert!(db_result.is_none(),
                                    "GET: Key {:?} not expected (not in data_states map) but found in DB: {:?}", key_bytes, db_result);
                            }
                        }
                        _ => {
                            // WRITE (1) or DELETE (2) operation
                            let value_str = format!(
                                "value_t{}_o{}_k{}",
                                thread_idx, ops_done_in_thread, key_idx
                            );
                            let value_bytes = ValueByte::from(value_str.as_bytes());
                            let op_type = if op_choice == 1 {
                                OpType::Write(value_bytes)
                            } else {
                                OpType::Delete
                            };

                            let key_entry_mutex = {
                                let mut data_states_guard = data_states_clone.write().unwrap(); // Write access to the HashMap (for entry/insert)
                                data_states_guard
                                    .entry(key_bytes.clone())
                                    .or_insert_with(|| Arc::new(Mutex::new(None)))
                                    .clone()
                            };

                            let mut data_state_entry_guard = key_entry_mutex.lock().unwrap();

                            let kvs_to_send = vec![(key_bytes.clone(), op_type.clone())];
                            let r_op = writer.put(kvs_to_send);

                            let committed_op_id = r_op.recv().expect(&format!(
                                "Thread {} operation {} failed to receive OpId for key {:?}",
                                thread_idx, ops_done_in_thread, key_bytes
                            ));
                            let new_kv_op = KVOpertion::new(
                                committed_op_id,
                                KeyVec::from(key_bytes.as_ref()),
                                op_type,
                            );
                            *data_state_entry_guard = Some(new_kv_op);
                        }
                    }
                }
                info!(
                    "Thread {} finished {} operations.",
                    thread_idx, ops_per_thread
                );
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Worker thread panicked");
        }

        let mut loop_count = 0;
        loop {
            if !db.need_freeze() && !db.need_dump() && !db.need_compact() {
                break;
            }
            loop_count += 1;
            if loop_count > 500 {
                panic!(
                    "Timeout waiting for background operations to complete. Freeze: {}, Dump: {}, Compact: {}",
                    db.need_freeze(), db.need_dump(), db.need_compact()
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(config.thread_sleep_time));
        }
        info!("Final background tasks completed.");

        info!(
            "Starting final verification against data_states (total keys: {}).",
            data_states.read().unwrap().len() // Read access to the HashMap
        );
        let final_reader = db.get_reader();
        let data_states_final_guard = data_states.read().unwrap(); // Read access to the HashMap

        for (key_bytes, key_entry_mutex) in data_states_final_guard.iter() {
            let data_state_entry_guard = key_entry_mutex.lock().unwrap();
            let db_result = final_reader.query(KeyVec::from(key_bytes.as_ref()));

            match (&*data_state_entry_guard, db_result) {
                (Some(expected_kv), Some(actual_kv)) => {
                    assert_eq!(
                        actual_kv.id, expected_kv.id,
                        "Final check: OpId mismatch for key {:?}. Actual ID: {}, Expected ID: {}",
                        key_bytes, actual_kv.id, expected_kv.id
                    );
                    assert_eq!(
                        actual_kv.key.as_ref(),
                        expected_kv.key.as_ref(),
                        "Final check: Key mismatch for {:?}",
                        key_bytes
                    );
                    assert_eq!(
                        actual_kv.op, expected_kv.op,
                        "Final check: OpType mismatch for {:?}",
                        key_bytes
                    );
                }
                (Some(expected_kv), None) => {
                    if matches!(expected_kv.op, OpType::Write(_)) {
                        panic!("Final check: Key {:?} expected to be found (Write), but not found. Expected: {:?}", key_bytes, expected_kv);
                    }
                }
                (None, Some(actual_kv)) => {
                    panic!(
                        "Final check: Key {:?} not expected (data_states `None`) but found: {:?}",
                        key_bytes, actual_kv
                    );
                }
                (None, None) => { /* Correct */ }
            }
        }
        info!(
            "Final verification completed successfully for {} keys.",
            data_states_final_guard.len()
        );
    }
}
