// block struct
// kv entry
// [id(u64),key_len,key_data,value_len,value_data]
// block
// [[entry],[entry],...[entry][count(u64)]]

use std::io::BufRead;
use std::iter::Peekable;
use std::ops::Not;
use std::u8;
use std::usize;

use crate::db::common::KeyQuery;
use crate::db::common::OpId;
use crate::db::common::OpType;
use crate::db::key::KeySlice;
use anyhow::Error;
use anyhow::Result;
use byteorder::WriteBytesExt;
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Bytes;
use bytes::BytesMut;
use std::io::{Cursor, Read, Write};

use crate::db::common::{new_buffer, Buffer, KVOpertion, KViterAgg};

pub const DATA_BLOCK_SIZE: usize = 4 * 1024;
use crate::db::key::KeyBytes;
use std::cell::RefCell;


pub struct BlockReader {
    data: Bytes,
    first_key: KeyBytes,
    count: usize,
}

impl BlockReader {
    pub fn new(data: Vec<u8>) -> Self {
        let data_len = data.len();
        // Read count from the last 8 bytes (u64)
        let count_bytes_start = data_len - std::mem::size_of::<u64>();
        let mut cursor = Cursor::new(&data[count_bytes_start..]);
        let count = cursor.read_u64::<LittleEndian>().unwrap() as usize;

        // The actual block data is everything before the count
        let data_bytes = Bytes::from(data);

        let bytes = data_bytes.slice(..count_bytes_start);

        // Decode the first KV operation to get the first key
        let (first_kv, _) = KVOpertion::decode(bytes);
        let first_key = first_kv.key;

        BlockReader {
            data: data_bytes,
            first_key,
            count,
        }
    }

    pub fn search(&self, key: &KeySlice, op_id: OpId) -> Option<(OpType, OpId)> {
        // The actual block data is everything before the last 8 bytes (count)
        let data_slice = &self
            .data
            .slice(..self.data.len() - std::mem::size_of::<u64>());
        let mut cursor = Cursor::new(data_slice);

        let mut result: Option<(OpType, OpId)> = None;
        let mut last_matching_id: OpId = 0; // Keep track of the highest ID found for the key <= op_id

        // Iterate through the KV operations in the block
        for _ in 0..self.count {
            // Decode the next KV operation from the cursor
            let (kv_op, _) = KVOpertion::decode(data_slice.slice(cursor.position() as usize..));

            // Move the cursor past the decoded KV operation
            cursor.set_position(cursor.position() + kv_op.encode_size() as u64);

            // Optimization: If the current key is greater than the target key,
            // we can stop searching as keys are sorted within the block.
            if kv_op.key.as_ref() > key.as_ref() {
                break; // No need to check further
            }

            if kv_op.key.as_ref().eq(key.as_ref()) {
                // Found the key, check if this operation's ID is relevant
                if kv_op.id <= op_id && kv_op.id >= last_matching_id {
                    // This is the latest relevant operation found so far for this key
                    result = Some((kv_op.op, kv_op.id)); // Update result with the operation type and its ID
                    last_matching_id = kv_op.id; // Update the latest ID found
                }
            }
        }
        result
    }
    pub fn to_iter(self) -> BlockIter {
        // Create a BlockIter, transferring ownership of the buffer and count.
        let bytes = Bytes::from(self.data);
        let count = self.count;
        BlockIter::new(bytes, count)
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.data.into()
    }
}

pub struct BlockBuilder {
    buffer: Buffer,
    last_key_positon: Option<u64>,
    count: u64,
}

impl BlockBuilder {
    pub fn new() -> Self {
        BlockBuilder {
            buffer: new_buffer(DATA_BLOCK_SIZE),
            last_key_positon: None,
            count: 0,
        }
    }

    pub fn last_key(&self) -> Option<KeyBytes> {
        if let Some(pos) = self.last_key_positon {
            // Create a slice from the buffer starting at the last_key_positon
            let buffer_ref = self.buffer.get_ref();
            let slice_from_last_key = buffer_ref[pos as usize..].to_vec();
            let data = Bytes::from(slice_from_last_key);

            // Decode the KV operation from this slice to get the key
            let (kv_op, _) = KVOpertion::decode(data);
            Some(kv_op.key) // Already KeyBytes
        } else {
            None
        }
    }
    pub fn is_empty(&self) -> bool {
        self.buffer.position() == 0
    }

    pub fn reset(&mut self) {
        self.last_key_positon = None;
        self.count = 0;
        self.buffer.set_position(0);
    }

    pub fn add(&mut self, op: KVOpertion) -> bool {
        let op_size = op.encode_size();
        // Check if adding the current operation would exceed the block size limit,
        // considering space for the final count (u64).
        if self.buffer.position() as usize + op_size + std::mem::size_of::<u64>() > DATA_BLOCK_SIZE
        {
            return false;
        }
        self.last_key_positon = Some(self.buffer.position());
        op.encode(&mut self.buffer);
        self.count += 1;
        true
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.buffer.into_inner()
    }
    pub fn get_ref(&self) -> &[u8] {
        self.buffer.get_ref().as_ref()
    }
    pub fn finish(&mut self) -> Result<&[u8]> {
        // Write count to the last 8 bytes of the block.
        // The total size of the block is DATA_BLOCK_SIZE.
        let count_pos = DATA_BLOCK_SIZE - std::mem::size_of::<u64>();
        self.buffer.set_position(count_pos as u64);
        self.buffer.write_u64::<LittleEndian>(self.count)?;

        Ok(self.buffer.get_ref().as_ref())
    }

    pub fn fill<T: Iterator<Item = KVOpertion>>(&mut self, it: &mut Peekable<T>) -> Result<&[u8]> {
        // Iterate through the peekable iterator and fill the block until it's full or the iterator is exhausted.
        // get first key by peek() if pit is empty return error with message "at least one kv"
        let first_kv = it
            .peek()
            .ok_or_else(|| Error::msg("Iterator must contain at least one KV operation"))?;
        let first_key = first_kv.key.clone();

        let mut current_size = 0;
        while let Some(item) = it.peek() {
            let size = item.encode_size();

            if current_size + size + 8 > DATA_BLOCK_SIZE {
                break; // Block is full
            }
            self.add(item.clone());
            current_size += size;
            //
            it.next(); // Consume the item
        }
        self.finish()
    }
}

// may contains same key but diff id
pub struct BlockIter {
    data: Bytes, // Owns the buffer now
    num: usize,
    count: usize,
    offset: usize,
}
impl Iterator for BlockIter {
    type Item = KVOpertion;
    fn next(&mut self) -> Option<Self::Item> {
        if self.count == self.num {
            return None;
        }
        let (res, offset) = KVOpertion::decode(self.data.slice(self.offset..));
        self.count += 1;
        self.offset += offset;
        Some(res)
    }
}
impl BlockIter {
    pub fn new(mut data: Bytes, num: usize) -> Self {
        // Takes ownership of buffer
        BlockIter {
            data,
            num,
            count: 0,
            offset: 0,
        }
    }

    pub fn into(self) -> Vec<u8> {
        self.data.to_vec()
    }
}

#[cfg(test)]
pub mod test {
    use super::BlockReader;
    use crate::db::block::BlockBuilder;
    use crate::db::block::DATA_BLOCK_SIZE;
    use crate::db::common::new_buffer;
    use crate::db::common::KViterAgg;
    use crate::db::common::OpId;
    use crate::db::common::{KVOpertion, OpType};
    use crate::db::key::KeySlice;
    use crate::db::key::KeyBytes;
    use byteorder::LittleEndian;
    use byteorder::WriteBytesExt;
    use std::io::Cursor;
    use std::ops::Range;
    use std::usize;

    use super::BlockIter;
    // pad zero at begin to length 6
    // panic if len of i is eq or more than 6
    // e.g. 11 ->"000011" 123456->"123456"
    pub fn pad_zero(i: u64) -> String {
        let s = i.to_string();
        let len = s.len();
        if len > 6 {
            panic!("Input number {} has more than 6 digits", i);
        }
        // Use format! to pad with leading zeros
        format!("{:0>6}", s)
    }
    pub fn create_kv_data_with_range(r: Range<usize>) -> Vec<KVOpertion> {
        create_kv_data_with_range_id_offset(r, 0)
    }
    pub fn create_kv_data_with_range_id_offset(r: Range<usize>, id: OpId) -> Vec<KVOpertion> {
        let mut v = Vec::new();

        for i in r {
            let tmp = KVOpertion::new(
                i as u64 + id,
                pad_zero(i as u64).as_bytes().into(),
                OpType::Write(i.to_string().as_bytes().into()),
            );
            v.push(tmp);
        }

        let it = v.iter();
        let mut out_it = it.map(|a| (&a.id, &a.key, &a.op));

        v
    }
    fn build_block_from_kvs_iter(kvs: Vec<KVOpertion>) -> BlockReader {
        let mut kv_iter = kvs.into_iter();
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();
        let mut block_builder = BlockBuilder::new();
        block_builder.fill(&mut kv_iter_agg).expect("fill error");
        BlockReader::new(block_builder.into_inner())
    }
    pub fn create_kv_data_in_range_zero_to(size: usize) -> Vec<KVOpertion> {
        create_kv_data_with_range(0..size)
    }

    #[test]
    fn test_block_reader_search() {
        let mut kvs = create_kv_data_in_range_zero_to(100);

        // Add a new KV with key "50" and a higher ID
        let key50_str = pad_zero(50);
        kvs.push(KVOpertion::new(
            10000, // Higher ID
            key50_str.as_bytes().into(),
            OpType::Write("51".as_bytes().into()), // New value
        ));

        // sort kvs by key
        kvs.sort_by(|a, b| a.key.cmp(&b.key));

        let mut br = build_block_from_kvs_iter(kvs); // Make br mutable

        // Test search for key "50" with op_id 10000
        let key50_slice = KeySlice::from(key50_str.as_bytes());
        let result50_high_id = br.search(&key50_slice.clone(), 10000);
        assert_eq!(
            result50_high_id,
            Some((OpType::Write("51".as_bytes().into()), 10000)),
            "Should find key '50' with value '51' for op_id 10000"
        );

        // Test search for key "50" with op_id 50 (should find the original entry)
        let result50_low_id = br.search(&key50_slice, 50);
        assert_eq!(
            result50_low_id,
            Some((OpType::Write(50.to_string().as_bytes().into()), 50)),
            "Should find key '50' with value '50' for op_id 50"
        );

        // Test search "10"
        let key10_str = pad_zero(10);
        let key10_slice = KeySlice::from(key10_str.as_bytes());
        // Assuming op_id for key "10" is 10, as per create_kv_data_in_range_zero_to
        let result10 = br.search(&key10_slice, 10);
        assert_eq!(
            result10,
            Some((OpType::Write(10.to_string().as_bytes().into()), 10)),
            "Should find key '10'"
        );

        let key0_str = pad_zero(0);
        let key0_slice = KeySlice::from(key0_str.as_bytes());
        let result0 = br.search(&key0_slice, 0); // op_id for key "0" is 0
        assert_eq!(
            result0,
            Some((OpType::Write(0.to_string().as_bytes().into()), 0)),
            "Should find key '0'"
        );

        // Test search "100" (not found)
        let key100_str = pad_zero(100); // This key is not in the 0..100 range
        let key100_slice = KeySlice::from(key100_str.as_bytes());
        // Use a relevant op_id, e.g., 100, though it shouldn't matter if the key isn't present.
        let result100 = br.search(&key100_slice, 100);
        assert!(result100.is_none(), "Should not find key '100'");
    }

    #[test]
    fn test_block_reader_search_with_same_key() {
        // Create initial kv data list [0..10)
        let mut kvs = create_kv_data_in_range_zero_to(10);

        // Add duplicate key KVs, inserting them after the existing key "5"
        // Find the position of the first element with key >= "5"
        let insert_pos = kvs
            .iter()
            .position(|kv| kv.key >= "5".to_string().as_bytes().into())
            .unwrap_or(kvs.len());

        kvs.insert(
            insert_pos,
            KVOpertion::new(
                15, // Insert the one with the higher ID first if needed, though search handles it
                5.to_string().as_bytes().into(),
                OpType::Write("duplicate_15".as_bytes().into()),
            ),
        );
        kvs.insert(
            insert_pos,
            KVOpertion::new(
                11,
                5.to_string().as_bytes().into(),
                OpType::Write("duplicate_5".as_bytes().into()),
            ),
        );

        let count = kvs.len();

        // Build buffer
        let mut block_builder = BlockBuilder::new();
        block_builder.fill(&mut kvs.into_iter().peekable()).unwrap();
        let block_reader = BlockReader::new(block_builder.into_inner());

        // Check search result for key "5" with a high enough op_id
        let f = 5.to_string();
        let search_key = KeySlice::from(f.as_bytes());
        let search_op_id = 20; // Ensure all versions are considered
        let result = block_reader.search(&search_key, search_op_id);

        // Assert that the latest version ("duplicate_15") is found
        assert_eq!(
            result,
            Some((OpType::Write("duplicate_15".as_bytes().into()), 15))
        );
    }

    #[test]
    fn test_block_builder() {
        // write kv, key range from string 0 to string 100 ,value is same as key, to BlockBuilder
        let iter = create_kv_data_in_range_zero_to(100);
        let br = build_block_from_kvs_iter(iter);
        let mut block_it = br.to_iter();

        let mut count = 0;
        for i in 0..100 {
            let kv_op = block_it.next().expect(&format!(
                "Iterator should have 100 items, failed at item {}",
                i
            ));
            assert_eq!(
                kv_op.key,
                pad_zero(i as u64).as_bytes().into(),
                "Key mismatch at index {}",
                i
            );
            assert_eq!(
                kv_op.op,
                OpType::Write(i.to_string().as_bytes().into()),
                "OpType mismatch at index {}",
                i
            );
            // Assuming OpId is also i as per create_kv_data_with_range
            assert_eq!(kv_op.id, i as u64, "OpId mismatch at index {}", i);
            count += 1;
        }
        assert_eq!(count, 100, "Expected to iterate over 100 items");
        assert!(
            block_it.next().is_none(),
            "Iterator should be exhausted after 100 items"
        );
    }

    #[test]
    fn test_block_reader_count() {
        let num_kvs = 100;
        let kvs = create_kv_data_in_range_zero_to(num_kvs);
        let br = build_block_from_kvs_iter(kvs);

        // Assert that the count method returns the correct number of key-value pairs
        assert_eq!(
            br.count(),
            num_kvs,
            "BlockReader count should match the number of KVs used to build it"
        );
    }

    #[test]
    fn test_block_reader_first_key() {
        let num_kvs = 100;
        let kvs = create_kv_data_in_range_zero_to(num_kvs);
        let br = build_block_from_kvs_iter(kvs.clone());

        // The first key should be the key from the first KV operation
        let expected_first_key = &kvs[0].key;

        // Assert that the first_key method returns the correct first key
        assert_eq!(
            br.first_key().as_ref(),
            expected_first_key.as_ref(),
            "BlockReader first_key should match the first key used to build it"
        );
    }

    #[test]
    fn test_block_iter() {
        let iter = create_kv_data_in_range_zero_to(100);
        let mut kv_iter = iter.into_iter();
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();
        let mut block_builder = BlockBuilder::new();
        block_builder.fill(&mut kv_iter_agg).unwrap();
        let br = BlockReader::new(block_builder.into_inner());
        let block_iter = br.to_iter(); // Pass ownership
        let mut count = 0;
        for (i, kv) in block_iter.enumerate() {
            assert_eq!(kv.key, pad_zero(i as u64).as_bytes().into());
            count += 1;
        }
        assert_eq!(count, 100);
    }

    #[test]
    fn test_block_builder_add_and_finish() {
        let num_kvs = 5;
        let kvs = create_kv_data_in_range_zero_to(num_kvs);

        let mut builder = BlockBuilder::new();
        for kv in kvs.iter() {
            builder.add(kv.clone());
        }

        builder.finish().expect("Failed to finish block");
        let block_reader = BlockReader::new(builder.into_inner());

        // Verify count
        assert_eq!(block_reader.count(), num_kvs, "BlockReader count mismatch");

        // Verify contents by iterating
        let mut block_iter = block_reader.to_iter();
        for (i, kv_read) in block_iter.enumerate() {
            let original_kv = &kvs[i];
            assert_eq!(kv_read.id, original_kv.id, "KV id mismatch at index {}", i);
            assert_eq!(
                kv_read.key.as_ref(),
                original_kv.key.as_ref(),
                "KV key mismatch at index {}",
                i
            );
            assert_eq!(
                kv_read.op, original_kv.op,
                "KV operation mismatch at index {}",
                i
            );
        }
    }

    #[test]
    fn test_block_reader_new() {
        // Create some KV operations
        let kvs = create_kv_data_in_range_zero_to(5); // Use 5 KVs

        // Build a block from these KVs
        let mut kv_iter = kvs.clone().into_iter();
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();
        let mut block_builder = BlockBuilder::new();
        block_builder
            .fill(&mut kv_iter_agg)
            .expect("Failed to build block");
        let block_data = block_builder.into_inner();

        // Create a BlockReader from the raw block data
        let block_reader = BlockReader::new(block_data);

        // Assert that the count and first key are correctly read
        assert_eq!(block_reader.count(), 5, "BlockReader should have 5 items");
        assert_eq!(
            block_reader.first_key().as_ref(),
            kvs[0].key.as_ref(),
            "BlockReader first key should match the first KV's key"
        );
    }

    #[test]
    fn test_block_builder_empty_iterator() {
        // Create an empty iterator
        let kvs: Vec<KVOpertion> = Vec::new();
        let mut kv_iter = kvs.into_iter();
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();

        // Build a block with the empty iterator - should result in an error
        let mut block_builder = BlockBuilder::new();
        let result = block_builder.fill(&mut kv_iter_agg);

        // Assert that the result is an error and contains the expected message
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Iterator must contain at least one KV operation"
        );
    }

    #[test]
    fn test_block_builder_with_oversized_data() {
        // Create a large dataset that exceeds DATA_BLOCK_SIZE
        let kvs = create_kv_data_in_range_zero_to(200); // Create more data than can fit in one block
        let mut kv_iter = kvs.clone().into_iter();
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();

        // Build a block with the oversized data
        let mut block_builder = BlockBuilder::new();
        block_builder.fill(&mut kv_iter_agg).unwrap();
        let br = BlockReader::new(block_builder.into_inner());

        // Verify that not all items were consumed (some remained in the iterator)
        assert!(
            kv_iter_agg.peek().is_some(),
            "Iterator should still have items"
        );

        // Verify the block's count is less than the total number of items
        assert!(
            br.count() < kvs.len(),
            "Block should contain fewer items than the total"
        );

        // Verify the first key matches
        assert_eq!(
            br.first_key().as_ref(),
            kvs[0].key.as_ref(),
            "First key should match"
        );

        // Verify we can iterate through all items in the block
        let br_count = br.count();
        let mut block_iter = br.to_iter();
        let mut count = 0;
        while let Some(_) = block_iter.next() {
            count += 1;
        }
        assert_eq!(count, br_count, "Iterator count should match block count");
    }

    #[test]
    fn test_block_builder_last_key() {
        let mut builder = BlockBuilder::new();

        // Initially, last_key should be None
        assert!(
            builder.last_key().is_none(),
            "last_key should be None for an empty builder"
        );

        // Add first KV
        let kv1 = KVOpertion::new(
            1,
            "key1".as_bytes().into(),
            OpType::Write("value1".as_bytes().into()),
        );
        builder.add(kv1.clone());
        assert_eq!(
            builder.last_key().unwrap().as_ref(),
            "key1".as_bytes(),
            "last_key should be 'key1'"
        );

        // Add second KV
        let kv2 = KVOpertion::new(
            2,
            "key2".as_bytes().into(),
            OpType::Write("value2".as_bytes().into()),
        );
        builder.add(kv2.clone());
        assert_eq!(
            builder.last_key().unwrap().as_ref(),
            "key2".as_bytes(),
            "last_key should be 'key2'"
        );

        // Add third KV
        let kv3 = KVOpertion::new(
            3,
            "key3".as_bytes().into(),
            OpType::Write("value3".as_bytes().into()),
        );
        builder.add(kv3.clone());
        assert_eq!(
            builder.last_key().unwrap().as_ref(),
            "key3".as_bytes(),
            "last_key should be 'key3'"
        );

        // Test with a block that becomes full
        let mut full_builder = BlockBuilder::new();
        let mut last_added_key: Option<KeyBytes> = None;
        for i in 0.. {
            let key_str = format!("key_{:0>6}", i);
            let value_str = format!("value_{}", i);
            let kv_op = KVOpertion::new(
                i as u64,
                key_str.as_bytes().into(),
                OpType::Write(value_str.as_bytes().into()),
            );

            if !full_builder.add(kv_op.clone()) {
                break; // Block is full
            }
            last_added_key = Some(KeyBytes::from(kv_op.key.as_ref()));
        }
        assert_eq!(
            full_builder.last_key().unwrap().as_ref(),
            last_added_key.unwrap().as_ref(),
            "last_key should be the last key added before full"
        );
    }

    #[test]
    fn test_block_builder_reset() {
        let mut builder = BlockBuilder::new();

        // Add some data to the builder
        let kv1 = KVOpertion::new(
            1,
            "key1".as_bytes().into(),
            OpType::Write("value1".as_bytes().into()),
        );
        builder.add(kv1.clone());

        // Verify state before reset
        assert!(
            !builder.is_empty(),
            "Builder should not be empty before reset"
        );
        assert_eq!(builder.count, 1, "Count should be 1 before reset");
        assert!(
            builder.last_key().is_some(),
            "last_key should be Some before reset"
        );
        assert!(
            builder.buffer.position() > 0,
            "Buffer position should be greater than 0 before reset"
        );

        // Call reset
        builder.reset();

        // Verify state after reset
        assert!(builder.is_empty(), "Builder should be empty after reset");
        assert_eq!(builder.count, 0, "Count should be 0 after reset");
        assert!(
            builder.last_key().is_none(),
            "last_key should be None after reset"
        );
        assert_eq!(
            builder.buffer.position(),
            0,
            "Buffer position should be 0 after reset"
        );

        // Try adding data again to ensure it's usable
        let kv2 = KVOpertion::new(
            2,
            "key2".as_bytes().into(),
            OpType::Write("value2".as_bytes().into()),
        );
        assert!(
            builder.add(kv2.clone()),
            "Should be able to add after reset"
        );
        assert_eq!(builder.count, 1, "Count should be 1 after adding again");
        assert_eq!(
            builder.last_key().unwrap().as_ref(),
            "key2".as_bytes(),
            "last_key should be 'key2' after adding again"
        );
    }

    #[test]
    fn test_block_builder_add_buffer_size_check() {
        let mut builder = BlockBuilder::new();
        let mut added_count = 0;

        // Create KV operations until `add` returns false
        for i in 0.. {
            let kv_op = KVOpertion::new(
                i as u64,
                pad_zero(i as u64).as_bytes().into(),
                OpType::Write(format!("value_{}", i).as_bytes().into()),
            );

            if !builder.add(kv_op.clone()) {
                // If add returns false, the block is full
                break;
            }
            added_count += 1;
        }

        // Assert that at least one item was added
        assert!(added_count > 0, "Should have added at least one item");

        // Try to add one more item, it should fail
        let last_kv_op = KVOpertion::new(
            added_count as u64,
            pad_zero(added_count as u64).as_bytes().into(),
            OpType::Write(format!("value_{}", added_count).as_bytes().into()),
        );
        assert!(
            !builder.add(last_kv_op.clone()),
            "Adding an item after block is full should return false"
        );

        // Finish the block and verify its properties
        builder.finish().expect("Failed to finish block");
        let block_reader = BlockReader::new(builder.into_inner());

        assert_eq!(
            block_reader.count(),
            added_count as usize,
            "BlockReader count should match the number of successfully added KVs"
        );

        // The final size of the block should be DATA_BLOCK_SIZE
        assert_eq!(
            block_reader.into_inner().len(),
            DATA_BLOCK_SIZE,
            "Final block size should be DATA_BLOCK_SIZE"
        );
    }
}
