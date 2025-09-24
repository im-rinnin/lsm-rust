// table 结构
// [<block>,<block>...<block><block_meta>...<block_meta><block_meta_offset(u64)><block_meta_count(u64)>]
// <block_meta>:[key_size,first_key,key_size,last_key]
use std::{
    io::{Cursor, Read, Seek, Write},
    sync::Arc,
    u64, usize,
};

use crate::db::{
    common::{new_buffer, Buffer, KVOperation, OpId, OpType},
    key::{KeyBytes, KeySlice},
    store::{Store, StoreId},
};
use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use bytes::BytesMut;
use tracing::{debug, info};

use super::{
    block::{BlockIter, BlockReader, DATA_BLOCK_SIZE},
    common::SearchResult,
};
const SSTABLE_DATA_SIZE_LIMIT: usize = 2 * 1024 * 1024;

const BLOCK_COUNT_LIMIT: usize = SSTABLE_DATA_SIZE_LIMIT / DATA_BLOCK_SIZE;
pub type ThreadSafeTableReader<T> = Arc<TableReader<T>>;

fn open_store<T: Store>(id: StoreId) -> T {
    T::open(id, "table", "data")
}
#[derive(PartialEq, Debug)]
struct BlockMeta {
    first_key: KeyBytes,
    last_key: KeyBytes,
}
pub struct TableReader<T: Store> {
    store: T,
    block_metas: Vec<BlockMeta>,
}
pub struct TableIter<'a, T: Store> {
    table: &'a TableReader<T>,
    current_block_num: usize,
    current_block_iter: Option<BlockIter>,
    // Reusable buffer for reading block data
    block_read_buffer: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub struct TableConfig {
    block_num_limit: usize,
}

pub struct TableBuilder<T: Store> {
    store: T,
    block_metas: Vec<BlockMeta>,
    config: TableConfig,
    current_block_first_key: Option<KeyBytes>,
    block_builder: BlockBuilder,
}

impl BlockMeta {
    pub fn encode(&self, w: &mut Buffer) -> std::io::Result<()> {
        let first_key_len = self.first_key.len() as u64;
        w.write_u64::<LittleEndian>(first_key_len)?;
        w.write_all(self.first_key.as_ref())?;

        // Encode last_key
        let last_key_len = self.last_key.len() as u64;
        w.write_u64::<LittleEndian>(last_key_len)?;
        w.write_all(self.last_key.as_ref())?;
        Ok(())
    }
    pub fn decode(r: &mut Buffer) -> std::io::Result<Self> {
        // Decode first_key
        let first_key_len = r.read_u64::<LittleEndian>()? as usize;
        let mut first_key_data = vec![0u8; first_key_len];
        r.read_exact(&mut first_key_data)?;
        let first_key = KeyBytes::from_vec(first_key_data);

        // Decode last_key
        let last_key_len = r.read_u64::<LittleEndian>()? as usize;
        let mut last_key_data = vec![0u8; last_key_len];
        r.read_exact(&mut last_key_data)?;
        let last_key = KeyBytes::from_vec(last_key_data);

        Ok(BlockMeta {
            first_key,
            last_key,
        })
    }
}

// for level 0,must consider multiple key with diff value/or delete
impl<T: Store> TableReader<T> {
    pub fn new(store_id: StoreId) -> Self {
        debug!(id = store_id, "table read new with id");
        let store = open_store::<T>(store_id);
        Self::new_with_store_for_test(store)
    }
    pub fn new_with_store_for_test(store: T) -> Self {
        Self::try_new_with_store_for_test(store)
            .expect("failed to construct TableReader from store")
    }
    pub fn try_new_with_store_for_test(store: T) -> anyhow::Result<Self> {
        let store_len = store.len();
        let mut buffer_for_meta_pointers = [0u8; 16]; // Buffer for offset and count (2 * u64)

        // Read block_meta_offset and block_meta_count from the end of the store
        store.read_at(&mut buffer_for_meta_pointers, store_len - 16);
        let mut cursor = Cursor::new(buffer_for_meta_pointers);

        let block_meta_start_offset = cursor.read_u64::<LittleEndian>()? as usize;
        let block_meta_count = cursor.read_u64::<LittleEndian>()? as usize;

        // Read all block metas
        let block_metas_data_len = store_len - block_meta_start_offset - 16;
        let mut block_metas_buffer = vec![0; block_metas_data_len];
        store.read_at(&mut block_metas_buffer, block_meta_start_offset);

        let mut block_metas_cursor = Cursor::new(block_metas_buffer);
        let mut block_metas = Vec::with_capacity(block_meta_count);

        for _ in 0..block_meta_count {
            block_metas.push(BlockMeta::decode(&mut block_metas_cursor)?);
        }

        Ok(TableReader { store, block_metas })
    }
    pub fn store_id(&self) -> StoreId {
        self.store.id()
    }
    // min and max key in table
    pub fn key_range(&self) -> (KeyBytes, KeyBytes) {
        if self.block_metas.is_empty() {
            panic!("table can't be empty")
        }
        let first_key = self.block_metas.first().unwrap().first_key.clone();
        let last_key = self.block_metas.last().unwrap().last_key.clone();
        (first_key, last_key)
    }
    pub fn to_iter(&self) -> TableIter<'_, T> {
        TableIter::new(self)
    }
    pub fn take(self) -> T {
        self.store
    }

    pub fn find(&self, key: &KeySlice, id: OpId) -> SearchResult {
        if self.block_metas.is_empty() {
            return None; // Table is empty, key cannot be found
        }

        // Check if the key is less than the first key of the first block.
        // If so, the key cannot be in this table.
        if let Some(first_meta) = self.block_metas.first() {
            if key.as_ref().lt(first_meta.first_key.as_ref()) {
                return None;
            }
        }
        // This branch should ideally not be reached if block_metas.is_empty() is checked first,

        // Check if the key is greater than the last key of the last block.
        // If so, the key cannot be in this table.
        if let Some(last_meta) = self.block_metas.last() {
            if key.as_ref().gt(last_meta.last_key.as_ref()) {
                return None;
            }
        }

        let mut best_op: Option<(OpType, OpId)> = None;

        // Find the index of the first block whose first_key is <= search key.
        // `partition_point` returns the index of the first element for which the predicate is false.
        // So, `idx` will be the first index where `meta.first_key > key`.
        // We want to start from the block *before* that, if it exists.
        let start_idx = self
            .block_metas
            .partition_point(|meta| meta.first_key.as_ref().le(key.as_ref()))
            .saturating_sub(1); // saturating_sub(1) handles the case where partition_point returns 0

        // Expand left to cover contiguous blocks whose key range still contains `key`.
        // Blocks are size-bounded, not key-run bounded. That means a long run of the
        // same logical key (multiple versions with different OpIds) can span multiple
        // adjacent blocks. Starting from the last block whose first_key <= key is a good
        // heuristic to skip irrelevant earlier blocks, but it can miss older versions if
        // the run actually started in a previous block. To handle this, expand left over
        // any contiguous blocks whose [first_key, last_key] still contain `key`.
        let mut left_idx = start_idx;
        while left_idx > 0 {
            let prev = &self.block_metas[left_idx - 1];
            if key.as_ref() < prev.first_key.as_ref() || key.as_ref() > prev.last_key.as_ref() {
                break;
            }
            left_idx -= 1;
        }

        // Iterate through blocks from the adjusted starting point.
        for i in left_idx..self.block_metas.len() {
            let block_meta = &self.block_metas[i];

            // Optimization: If the current block's first key is already greater than the search key,
            // then the key cannot be in this block or any subsequent blocks (since block_metas are sorted by first_key).
            if key.as_ref().lt(block_meta.first_key.as_ref()) {
                break;
            }

            // Optimization: If the search key is greater than the last key of the current block,
            // then the key cannot be in this block. Continue to the next.
            if key.as_ref().gt(block_meta.last_key.as_ref()) {
                continue;
            }

            // If we reach here, the block's key range [first_key, last_key] potentially contains the search key.
            let offset = i * DATA_BLOCK_SIZE;
            // Read block into a BytesMut buffer and convert to Bytes without copying
            let mut data = BytesMut::with_capacity(DATA_BLOCK_SIZE);
            data.resize(DATA_BLOCK_SIZE, 0);
            self.store.read_at(&mut data[..], offset);
            let block_reader = BlockReader::from_bytes(data.freeze());

            if let Some((current_op_type, op_id_found)) = block_reader.search(key, id) {
                // current_op_type is already OpType, no need for conversion
                match best_op {
                    Some((_, current_best_id)) => {
                        if op_id_found > current_best_id {
                            best_op = Some((current_op_type, op_id_found));
                        }
                    }
                    None => {
                        best_op = Some((current_op_type, op_id_found));
                    }
                }
            }
        }
        best_op
    }
}

impl<'a, T: Store> TableIter<'a, T> {
    pub fn new(table: &'a TableReader<T>) -> Self {
        TableIter {
            table,
            current_block_num: 0,
            current_block_iter: None,
            block_read_buffer: Some(vec![0; DATA_BLOCK_SIZE]), // Initialize reusable buffer
        }
    }
}
impl<'a, T: Store> Iterator for TableIter<'a, T> {
    type Item = KVOperation;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we have a current block iterator, try to get the next item from it
            if let Some(block_iter) = &mut self.current_block_iter {
                if let Some(kv_op) = block_iter.next() {
                    return Some(kv_op);
                } else {
                    // Block iterator is exhausted, take its buffer back for reuse
                    let mut res = self.current_block_iter.take().unwrap().into();
                    self.block_read_buffer = Some(res);
                }
            }

            // Current block iterator is exhausted or not yet initialized, move to the next block
            if self.current_block_num >= self.table.block_metas.len() {
                return None; // No more blocks to process
            }

            let mut data = self.block_read_buffer.take().unwrap();

            // Load the next block into the reusable buffer
            let offset = self.current_block_num * DATA_BLOCK_SIZE;
            self.table.store.read_at(&mut data, offset);
            let block_reader = BlockReader::new(data); // Clone to pass ownership to BlockReader

            // Increment block number for the next iteration
            self.current_block_num += 1;

            // Set the new block iterator and loop to try getting an item from it
            self.current_block_iter = Some(block_reader.to_iter());
        }
    }
}

use crate::db::block::BlockBuilder;
use serde::{Deserialize, Serialize};
impl TableConfig {
    pub fn new(table_size: usize) -> Self {
        TableConfig {
            block_num_limit: table_size / DATA_BLOCK_SIZE,
        }
    }
    pub fn new_for_test() -> Self {
        TableConfig {
            block_num_limit: 1024 * 1024 / DATA_BLOCK_SIZE,
        }
    }
    pub fn set_table_size(&mut self, size: usize) {
        self.block_num_limit = size / DATA_BLOCK_SIZE;
    }
}
impl Default for TableConfig {
    fn default() -> Self {
        TableConfig {
            block_num_limit: BLOCK_COUNT_LIMIT,
        }
    }
}

impl<T: Store> TableBuilder<T> {
    pub fn new_with_store_for_test(store: T) -> Self {
        debug!(id = store.id(), "table_builder new with store");
        Self {
            store,
            block_metas: Vec::new(),
            config: TableConfig::default(),
            current_block_first_key: None,
            block_builder: BlockBuilder::new(),
        }
    }
    pub fn new_with_id_config(store_id: StoreId, c: TableConfig) -> Self {
        debug!(id = store_id, "table_builder new with store");
        Self {
            store: open_store(store_id),
            block_metas: Vec::new(),
            config: c,
            current_block_first_key: None,
            block_builder: BlockBuilder::new(),
        }
    }

    pub fn new_with_id(store_id: StoreId) -> Self {
        debug!(id = store_id, "table_builder new with store");
        Self {
            store: open_store(store_id),
            block_metas: Vec::new(),
            config: TableConfig::default(),
            current_block_first_key: None,
            block_builder: BlockBuilder::new(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.block_metas.is_empty() && self.block_builder.is_empty()
    }

    /// Adds a key-value operation to the current block.
    /// If the current block becomes full, it flushes the block to the store
    /// and starts a new block.
    /// Returns `false` if the operation cannot be added because the table
    /// has reached its `block_num_limit`.
    pub fn add(&mut self, op: KVOperation) -> bool {
        // Try to add the operation to the current block builder first.
        if self.block_builder.add(op.clone()) {
            if self.current_block_first_key.is_none() {
                self.current_block_first_key = Some(op.key.clone());
            }
            return true;
        }

        // If adding fails, the current block is full (or the op is too large for remaining space). Flush it.
        self.flush_current_block();

        // Check if we've reached the overall block limit for the table.
        if self.block_metas.len() >= self.config.block_num_limit {
            info!(block_limit = self.config.block_num_limit, "block_num_limit");
            return false; // Cannot add more blocks to this table.
        }

        // Create a new block builder and try adding the operation to it.
        self.block_builder = BlockBuilder::new();
        if !self.block_builder.add(op.clone()) {
            // The op itself does not fit in an empty block.
            return false;
        }
        self.current_block_first_key = Some(op.key);
        true
    }

    /// Flushes the current `BlockBuilder`'s content to the underlying store.
    ///
    /// This function takes the data from the `block_builder`, appends it to the `store`,
    /// creates a `BlockMeta` entry for the flushed block, and resets the `block_builder`.
    /// It is called when a block is full or when the entire table is being flushed.
    ///
    /// # Arguments
    /// * `last_key` - The last key contained in the block being flushed.
    fn flush_current_block(&mut self) {
        if self.block_builder.is_empty() {
            return; // Nothing to flush
        }

        let last_key = self
            .block_builder
            .last_key()
            .expect("Block should have a last key if it's full and add failed");

        let first_key = self
            .current_block_first_key
            .take()
            .expect("First key must be set when flushing a non-empty block");

        self.block_builder.finish().expect("block finish error");
        let block_buffer = self.block_builder.get_ref();

        // Append the block data to the store
        self.store.append(block_buffer);

        // Add block metadata
        self.block_metas.push(BlockMeta {
            first_key,
            last_key,
        });
        self.block_builder.reset();

        // Reset the buffer for the next block (already done if `add` created a new one)
        // or prepare for the next operation if this was the last flush.
    }

    pub fn flush(mut self) -> TableReader<T> {
        // Flush any remaining data in the current buffer
        if !self.block_builder.is_empty() {
            // Ensure last key exists if block is not empty
            assert!(
                self.block_builder.last_key().is_some(),
                "Block should have a last key if it's not empty"
            );
            self.flush_current_block();
        }

        // Calculate the offset where block metadata will start
        let block_meta_start_offset = self.store.len();

        // Delegate to fallible variant and unwrap at wrapper boundary
        self.try_flush_at(block_meta_start_offset)
            .expect("failed to flush table")
    }

    pub fn try_flush_at(
        mut self,
        block_meta_start_offset: usize,
    ) -> anyhow::Result<TableReader<T>> {
        // Encode and append all block metadata to the store tightly (no padding)
        let block_meta_count = self.block_metas.len() as u64;
        // Use a growable buffer to avoid prefilled zeros and gaps
        let mut temp_buffer = Cursor::new(Vec::new());
        for meta in &self.block_metas {
            meta.encode(&mut temp_buffer)?;
        }
        // Write trailer immediately after metas: [meta_start_offset][meta_count]
        temp_buffer.write_u64::<LittleEndian>(block_meta_start_offset as u64)?;
        temp_buffer.write_u64::<LittleEndian>(block_meta_count)?;
        self.store.append(temp_buffer.get_ref());

        // Flush the store to ensure all data is written to disk
        self.store.flush();

        // Create and return a TableReader
        Ok(TableReader {
            store: self.store,
            block_metas: self.block_metas,
        })
    }
    pub fn fill_with_op<'a, IT: Iterator<Item = &'a KVOperation>>(&mut self, it: IT) {
        for op_ref in it {
            if !self.add(op_ref.clone()) {
                // If add returns false, it means the table is full (block_num_limit reached)
                // and we cannot add more operations.
                break;
            }
        }
    }
}

#[cfg(test)]
pub mod test {

    use super::TableBuilder;
    use crate::db::block::test::create_kv_data_with_range_id_offset;
    use crate::db::common::OpId;
    use crate::db::key::KeyBytes;
    use crate::db::key::KeySlice;
    use crate::db::table::open_store;
    use std::mem;
    use std::ops::Range;

    use super::super::block::test::pad_zero;
    use crate::db::{
        common::{KVOperation, OpType},
        store::{Memstore, Store},
    };

    use super::super::block::test::create_kv_data_in_range_zero_to;
    use super::TableReader;
    pub fn create_test_table(range: Range<usize>) -> TableReader<Memstore> {
        create_test_table_with_id_offset(range, 0)
    }
    pub fn create_test_table_with_id_offset(
        range: Range<usize>,
        id: OpId,
    ) -> TableReader<Memstore> {
        let v = create_kv_data_with_range_id_offset(range, id);
        let store_id = 1u64; // Assign a unique u64 ID for this test helper
        let mut it = v.iter();
        let mut table = TableBuilder::new_with_id(store_id);
        table.fill_with_op(it);
        let res = table.flush();
        res
    }
    fn create_test_table_with_size(size: usize) -> TableReader<Memstore> {
        create_test_table(0..size)
    }

    fn create_test_kvs_for_add_test() -> Vec<KVOperation> {
        vec![
            KVOperation::new(
                0,
                "keyA".as_bytes().into(),
                OpType::Write("valueA".as_bytes().into()),
            ),
            KVOperation::new(
                1,
                "keyB".as_bytes().into(),
                OpType::Write("valueB".as_bytes().into()),
            ),
            KVOperation::new(
                2,
                "keyC".as_bytes().into(),
                OpType::Write("valueC".as_bytes().into()),
            ),
        ]
    }

    #[test]
    fn test_table_reader_new() {
        let num_kvs = 1000;
        let kvs = create_kv_data_in_range_zero_to(num_kvs);

        let store_id = 100u64; // Assign a unique u64 ID for this test
        let store = Memstore::open_for_test(store_id);
        let mut table_builder = TableBuilder::new_with_store_for_test(store);

        table_builder.fill_with_op(kvs.iter());
        let table_reader = table_builder.flush();
        let store = table_reader.take();
        let table_reader = TableReader::new_with_store_for_test(store);

        // Verify that keys can be found in the new table reader
        for i in 0..num_kvs {
            let key_str = pad_zero(i as u64);
            let key_slice = &KeySlice::from(key_str.as_bytes());
            let result = table_reader.find(key_slice, i as u64);
            assert_eq!(
                result,
                Some((OpType::Write(i.to_string().as_bytes().into()), i as u64)),
                "Should find key '{}'",
                i
            );
        }

        // Test a key that should not be found (outside the range)
        let key_not_found_str = pad_zero(num_kvs as u64);
        let key_not_found_slice = &KeySlice::from(key_not_found_str.as_bytes());
        let result_not_found = table_reader.find(key_not_found_slice, num_kvs as u64);
        assert!(
            result_not_found.is_none(),
            "Should not find key '{}'",
            num_kvs
        );
    }

    #[test]
    fn test_key_range() {
        // Test case 1: Non-empty table
        let num_kvs = 100;
        let table = create_test_table(0..num_kvs);
        let (min_key, max_key) = table.key_range();
        assert_eq!(min_key, KeyBytes::from("000000".as_bytes()));
        assert_eq!(
            max_key,
            KeyBytes::from(pad_zero((num_kvs - 1) as u64).as_bytes())
        );
    }

    #[test]
    fn test_table_build_add() {
        let kvs = create_test_kvs_for_add_test();

        let store_id = 101u64; // Assign a unique u64 ID for this test
        let mut tb = TableBuilder::<Memstore>::new_with_id(store_id);

        for kv_op in &kvs {
            tb.add(kv_op.clone());
        }
        let table_reader = tb.flush();

        // Check block metas
        assert_eq!(table_reader.block_metas.len(), 1);
        let block_meta = &table_reader.block_metas[0];
        assert_eq!(block_meta.first_key.as_ref(), "keyA".as_bytes());
        assert_eq!(block_meta.last_key.as_ref(), "keyC".as_bytes());

        // Check table contents by iterating
        let mut collected_kvs = Vec::new();
        let table_iter = table_reader.to_iter();
        for kv_op_read in table_iter {
            collected_kvs.push(kv_op_read);
        }

        assert_eq!(collected_kvs.len(), kvs.len());
        for (idx, original_kv) in kvs.iter().enumerate() {
            assert_eq!(collected_kvs[idx].id, original_kv.id);
            assert_eq!(collected_kvs[idx].key, original_kv.key);
            assert_eq!(collected_kvs[idx].op, original_kv.op);
        }
    }
    #[test]
    fn test_table_build_fill() {
        let kvs = create_test_kvs_for_add_test();

        let store_id = 102u64; // Assign a unique u64 ID for this test
        let mut tb = TableBuilder::<Memstore>::new_with_id(store_id);

        let kvs_iter = kvs.iter();
        tb.fill_with_op(kvs_iter);
        let table_reader = tb.flush();

        // Check block metas
        assert_eq!(table_reader.block_metas.len(), 1);
        let block_meta = &table_reader.block_metas[0];
        assert_eq!(block_meta.first_key.as_ref(), "keyA".as_bytes());
        assert_eq!(block_meta.last_key.as_ref(), "keyC".as_bytes());

        // Check table contents by iterating
        let mut collected_kvs = Vec::new();
        let table_iter = table_reader.to_iter();
        for kv_op in table_iter {
            collected_kvs.push(kv_op);
        }

        assert_eq!(collected_kvs.len(), kvs.len());
        for (idx, original_kv) in kvs.iter().enumerate() {
            assert_eq!(collected_kvs[idx].id, original_kv.id);
            assert_eq!(collected_kvs[idx].key, original_kv.key);
            assert_eq!(collected_kvs[idx].op, original_kv.op);
        }
    }

    #[test]
    fn test_table_build_use_large_iter_and_check_by_iterator() {
        let num = 70000;
        //test data more than table
        let kvs = create_kv_data_in_range_zero_to(num);

        let store_id = 103u64; // Assign a unique u64 ID for this test
        let mut tb = TableBuilder::<Memstore>::new_with_id(store_id);
        tb.fill_with_op(kvs.iter());
        let table = tb.flush();

        let meta = &table.block_metas;
        let last_key = &meta.last().expect("block_metas empty").last_key;

        let table_iter = table.to_iter();
        let mut kv_index = 0; // Track index in the original kvs Vec

        for kv in table_iter {
            // Find the corresponding original kv operation
            // This assumes the order is preserved, which should be the case
            // if KViterAgg sorts correctly.
            let original_kv = &kvs[kv_index];

            assert_eq!(kv.id, original_kv.id);
            assert_eq!(kv.key, original_kv.key);
            assert_eq!(kv.op, original_kv.op);
            kv_index += 1;
        }
        // table has not enough space to save all kv
        assert!(kv_index < num);
        // check kv num
        let lasy_key_string = last_key.to_string();
        assert_eq!(kv_index, lasy_key_string.parse::<usize>().unwrap() + 1);
    }

    #[test]
    fn test_table_reader_find_with_duplicate_key() {
        // Create initial kv data list [0..10)
        let mut kvs = create_kv_data_in_range_zero_to(10);

        // Add duplicate key KVs for key "000005" with different OpIds and values
        let key_to_duplicate = pad_zero(5);

        // Version 1: id 5 (original from create_kv_data_in_range_zero_to)
        // Version 2: id 15, value "duplicate_15"
        kvs.push(KVOperation::new(
            15,
            key_to_duplicate.as_bytes().into(),
            OpType::Write("duplicate_15".as_bytes().into()),
        ));

        // Version 3: id 11, value "duplicate_11"
        kvs.push(KVOperation::new(
            11,
            key_to_duplicate.as_bytes().into(),
            OpType::Write("duplicate_11".as_bytes().into()),
        ));

        // Sort kvs by key, then by OpId (important for block building and search logic)
        kvs.sort_by(|a, b| {
            if a.key == b.key {
                a.id.cmp(&b.id)
            } else {
                a.key.cmp(&b.key)
            }
        });

        let store_id = 104u64; // Assign a unique u64 ID for this test
        let mut tb = TableBuilder::<Memstore>::new_with_id(store_id);
        tb.fill_with_op(kvs.iter());
        let table_reader = tb.flush();

        // Search for the duplicated key with an OpId that should pick the latest version (id 15)
        let search_key = KeySlice::from(key_to_duplicate.as_bytes());
        let search_op_id = 20; // An OpId higher than all versions

        let result = table_reader.find(&search_key, search_op_id);

        // Assert that the latest version ("duplicate_15") is found
        assert_eq!(
            result,
            Some((OpType::Write("duplicate_15".as_bytes().into()), 15)),
            "Should find the latest version of the duplicate key"
        );

        // Search for the duplicated key with an OpId that should pick the middle version (id 11)
        let search_op_id_middle = 12; // An OpId between 5 and 15, but higher than 11
        let result_middle = table_reader.find(&search_key, search_op_id_middle);
        assert_eq!(
            result_middle,
            Some((OpType::Write("duplicate_11".as_bytes().into()), 11)),
            "Should find the middle version of the duplicate key"
        );

        // Search for the duplicated key with an OpId that should pick the original version (id 5)
        let search_op_id_original = 5; // An OpId equal to the original version
        let result_original = table_reader.find(&search_key, search_op_id_original);
        assert_eq!(
            result_original,
            Some((OpType::Write(5.to_string().as_bytes().into()), 5)),
            "Should find the original version of the duplicate key"
        );

        // Test a key that was not duplicated
        let key_non_duplicate = pad_zero(1);
        let search_key_non_duplicate = KeySlice::from(key_non_duplicate.as_bytes());
        let result_non_duplicate = table_reader.find(&search_key_non_duplicate, 100);
        assert_eq!(
            result_non_duplicate,
            Some((OpType::Write(1.to_string().as_bytes().into()), 1)),
            "Should find the non-duplicated key"
        );
    }

    #[test]
    fn test_table_reader_find_with_duplicate_key_spanning_blocks() {
        // Build a table where the same key appears many times so that
        // its versions span multiple blocks. Then verify lookups for
        // both a very large op_id (latest) and a very small op_id (earliest)
        // succeed across block boundaries.

        // Choose a fixed key and generate many versions with increasing ids
        let key_str = pad_zero(123);

        // Create many operations for the same key with relatively large values
        // so we cross DATA_BLOCK_SIZE and span multiple blocks.
        // Each op is ~ (id+key_len+key+type+val_len+val) bytes; a 100-byte value
        // makes it easy to exceed a 4KB block within a few dozen entries.
        let versions = 300usize;
        let value_payload = vec![b'v'; 100];
        let mut ops: Vec<KVOperation> = Vec::with_capacity(versions);
        for i in 0..versions {
            ops.push(KVOperation::new(
                i as u64,
                KeyBytes::from(key_str.as_bytes()),
                OpType::Write(value_payload.as_slice().into()),
            ));
        }

        // Table build
        let store_id = 105u64; // unique id for this test
        let mut tb = TableBuilder::<Memstore>::new_with_id(store_id);
        tb.fill_with_op(ops.iter());
        let table_reader = tb.flush();

        // Ensure we indeed spanned at least two blocks
        assert!(
            table_reader.block_metas.len() >= 2,
            "Expected multiple blocks for the same key"
        );

        // Search with a very large op_id to get the latest version
        let search_key = KeySlice::from(key_str.as_bytes());
        let latest = table_reader
            .find(&search_key, u64::MAX)
            .expect("should find latest version across blocks");
        assert_eq!(latest.1, (versions - 1) as u64);

        // Search with a very small op_id that should be located in the earliest block
        let earliest = table_reader
            .find(&search_key, 0)
            .expect("should find earliest version across blocks");
        assert_eq!(earliest.1, 0u64);
    }

    #[test]
    fn test_table_builder_add_oversized_op_does_not_panic_and_returns_false() {
        use crate::db::block::DATA_BLOCK_SIZE;
        use crate::db::common::OpType;
        use crate::db::store::Memstore;

        // Construct a KVOperation whose encoded size cannot fit in an empty block.
        // op_size = 8(id) + 8(key_len) + key_len + 1(tag) + 8(val_len) + val_len
        // For DATA_BLOCK_SIZE=4096, BlockBuilder reserves 8 bytes for count,
        // so require op_size > 4088.
        let key = b"k"; // key_len = 1
        let min_value_len_overflow = (DATA_BLOCK_SIZE - 8) - (8 + 8 + key.len() + 1 + 8) + 1; // 4088 - (25 + key_len) + 1
        assert!(min_value_len_overflow > 0);
        let value = vec![b'x'; min_value_len_overflow];

        let op = KVOperation::new(
            1,
            KeyBytes::from(key.as_ref()),
            OpType::Write(value.as_slice().into()),
        );

        let store_id = 106u64;
        let mut tb = TableBuilder::<Memstore>::new_with_id(store_id);

        // Adding an oversized op should return false and not panic.
        let added = tb.add(op);
        assert!(!added, "oversized op should not be added to an empty block");

        // Flushing an empty builder should also not panic and produce an empty table.
        let table_reader = tb.flush();
        assert_eq!(table_reader.block_metas.len(), 0);
    }
    #[test]
    fn test_table_reader_find() {
        let len = 100;
        let table = create_test_table_with_size(len);

        for i in 0..len {
            let (op_type_found, op_id_found) = table
                .find(&KeySlice::from(pad_zero(i as u64).as_bytes()), i as u64)
                .expect(&format!("{} should in table", i));
            assert_eq!(
                op_type_found,
                OpType::Write(i.to_string().as_bytes().into())
            );
            assert_eq!(op_id_found, i as u64);
        }

        let res = table.find(&KeySlice::from("100".as_bytes()), 0);
        assert!(res.is_none());

        // Test finding key at the beginning of the range
        let (op_type_start, op_id_start) = table
            .find(&KeySlice::from(pad_zero(0 as u64).as_bytes()), 0 as u64)
            .expect("0 should in table");
        assert_eq!(
            op_type_start,
            OpType::Write(0.to_string().as_bytes().into())
        );
        assert_eq!(op_id_start, 0 as u64);

        // Test finding key at the end of the range
        let (op_type_end, op_id_end) = table
            .find(&KeySlice::from(pad_zero(99 as u64).as_bytes()), 99 as u64)
            .expect("99 should in table");
        assert_eq!(op_type_end, OpType::Write(99.to_string().as_bytes().into()));
        assert_eq!(op_id_end, 99 as u64);

        // Test finding key in the middle of the range
        let (op_type_middle, op_id_middle) = table
            .find(&KeySlice::from(pad_zero(50 as u64).as_bytes()), 50 as u64)
            .expect("50 should in table");
        assert_eq!(
            op_type_middle,
            OpType::Write(50.to_string().as_bytes().into())
        );
        assert_eq!(op_id_middle, 50 as u64);

        // Test searching for a key with a higher OpId (should still find the existing one)
        let (op_type_higher_id, op_id_higher_id) = table
            .find(&KeySlice::from(pad_zero(10 as u64).as_bytes()), 100 as u64) // Use a higher ID
            .expect("10 should still be in table with higher ID");
        assert_eq!(
            op_type_higher_id,
            OpType::Write(10.to_string().as_bytes().into())
        );
        assert_eq!(op_id_higher_id, 10 as u64); // The op_id found should be the original 10, not the queried 100

        // Test searching for a key below the range
        let res_below = table.find(&KeySlice::from("-1".as_bytes()), 0);
        assert!(res_below.is_none());
    }

    #[test]
    fn test_block_meta_encode_decode() {
        use super::BlockMeta;
        use crate::db::common::new_buffer;
        use crate::db::key::KeyBytes;
        use std::io::Seek;

        let original_meta = BlockMeta {
            first_key: KeyBytes::from_vec(b"key_a".to_vec()),
            last_key: KeyBytes::from_vec(b"key_z".to_vec()),
        };

        let mut buffer = new_buffer(1024); // Sufficiently large buffer
        original_meta.encode(&mut buffer).unwrap();

        // Reset buffer position to the beginning for decoding
        buffer.seek(std::io::SeekFrom::Start(0)).unwrap();

        let decoded_meta = BlockMeta::decode(&mut buffer).unwrap();

        assert_eq!(original_meta, decoded_meta);
    }
}
