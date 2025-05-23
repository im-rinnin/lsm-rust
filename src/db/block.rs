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
use crate::db::key::ValueVec;
use anyhow::Error;
use anyhow::Result;
use byteorder::WriteBytesExt;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Read, Write};

use crate::db::common::{new_buffer, Buffer, KVOpertion, KViterAgg}; // Removed write_kv_operion

pub const DATA_BLOCK_SIZE: usize = 4 * 1024;
use crate::db::key::KeyVec;
use std::cell::RefCell;

pub struct BlockReader {
    data: RefCell<Buffer>,
    first_key: KeyVec,
    count: usize,
}

impl BlockReader {
    fn new(data: Vec<u8>) -> Self {
        // read count from last u64 of data
        let count_pos = data.len() - 8;
        let mut cursor = Cursor::new(data);
        cursor.set_position(count_pos as u64);
        let count = cursor
            .read_u64::<LittleEndian>()
            .expect("Failed to read count from block");

        // read first key from data from data beginning
        cursor.set_position(0);
        let first_kv = KVOpertion::decode(&mut cursor);
        let first_key = first_kv.key;

        // return self
        BlockReader {
            data: RefCell::new(cursor),
            first_key,
            count: count as usize,
        }
    }

    pub fn search(&self, key: KeySlice, op_id: OpId) -> Option<OpType> {
        // Reset buffer position to the beginning for search
        self.data.borrow_mut().set_position(0);
        let mut result: Option<OpType> = None;
        let mut last_matching_id: OpId = 0; // Keep track of the highest ID found for the key <= op_id

        for _ in 0..self.count {
            let mut t = self.data.borrow_mut();
            let kv_op = KVOpertion::decode(&mut t);

            // Optimization: If the current key is greater than the target key,
            // we can stop searching as keys are sorted within the block.
            if kv_op.key.as_ref() > key.as_ref() {
                break; // No need to check further
            }

            if kv_op.key.as_ref().eq(key.as_ref()) {
                // Found the key, check if this operation's ID is relevant
                if kv_op.id <= op_id && kv_op.id >= last_matching_id {
                    // This is the latest relevant operation found so far for this key
                    result = Some(kv_op.op.clone()); // Update result with the operation type
                    last_matching_id = kv_op.id; // Update the latest ID found
                }
            }
        }
        result // Return the OpType of the latest relevant operation found, if any
    }
    pub fn to_iter(self) -> BlockIter {
        // Create a BlockIter, transferring ownership of the buffer and count.
        let buffer = self.data.into_inner();
        let count = self.count;
        BlockIter::new(buffer, count)
    }

    pub fn first_key(&self) -> &KeyVec {
        &self.first_key
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn inner(self) -> Vec<u8> {
        self.data.take().into_inner()
    }
}

pub struct BlockBuilder {
    buffer: Buffer,
    count: u64,
}

impl BlockBuilder {
    fn new() -> Self {
        BlockBuilder {
            buffer: new_buffer(DATA_BLOCK_SIZE),
            count: 0,
        }
    }

    fn fill<'a, T: Iterator<Item = &'a KVOpertion>>(
        mut self,
        it: &mut Peekable<T>,
    ) -> Result<Buffer> {
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
            item.encode(&mut self.buffer);
            current_size += size;
            self.count += 1;
            it.next(); // Consume the item
        }
        // Write count to the last 8 bytes of the block.
        // The total size of the block is DATA_BLOCK_SIZE.
        let count_pos = DATA_BLOCK_SIZE - std::mem::size_of::<u64>();
        self.buffer.set_position(count_pos as u64);
        self.buffer.write_u64::<LittleEndian>(self.count)?;

        Ok(self.buffer)
    }
}

// may contains same key but diff id
pub struct BlockIter {
    buffer: Buffer, // Owns the buffer now
    count: usize,
    current: usize,
}
impl Iterator for BlockIter {
    type Item = KVOpertion;
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.count {
            return None;
        }
        let res = KVOpertion::decode(&mut self.buffer);
        self.current += 1;
        return Some(res);
    }
}
impl BlockIter {
    pub fn new(mut buffer: Buffer, count: usize) -> Self {
        // Takes ownership of buffer
        buffer.set_position(0);
        BlockIter {
            buffer,
            count,
            current: 0,
        }
    }
}

#[cfg(test)]
pub mod test {
    use super::BlockReader;
    use crate::db::block::BlockBuilder;
    use crate::db::block::DATA_BLOCK_SIZE;
    use crate::db::common::new_buffer;
    use crate::db::common::KViterAgg;
    use crate::db::common::{KVOpertion, OpType};
    use crate::db::key::KeySlice;
    use crate::db::key::KeyVec;
    use byteorder::LittleEndian;
    use byteorder::WriteBytesExt;
    use std::io::Cursor;
    use std::ops::Range;

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
        let mut v = Vec::new();

        for i in r {
            let tmp = KVOpertion::new(
                i as u64,
                pad_zero(i as u64).as_bytes().into(),
                OpType::Write(i.to_string().as_bytes().into()),
            );
            v.push(tmp);
        }

        let it = v.iter();
        let mut out_it = it.map(|a| (&a.id, &a.key, &a.op));

        v
    }
    fn build_block_from_kvs_iter<'a>(
        kvs: &'a [KVOpertion], // Take a slice to create an iterator
    ) -> BlockReader {
        let mut kv_iter = kvs.iter();
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();
        let block_builder = BlockBuilder::new();
        let buffer = block_builder.fill(&mut kv_iter_agg).expect("fill error");
        BlockReader::new(buffer.into_inner())
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

        let mut br = build_block_from_kvs_iter(&kvs); // Make br mutable

        // Test search for key "50" with op_id 10000
        let key50_slice = KeySlice::from(key50_str.as_bytes());
        let result50_high_id = br.search(key50_slice.clone(), 10000);
        assert_eq!(
            result50_high_id,
            Some(OpType::Write("51".as_bytes().into())),
            "Should find key '50' with value '51' for op_id 10000"
        );

        // Test search for key "50" with op_id 50 (should find the original entry)
        let result50_low_id = br.search(key50_slice, 50);
        assert_eq!(
            result50_low_id,
            Some(OpType::Write(50.to_string().as_bytes().into())),
            "Should find key '50' with value '50' for op_id 50"
        );

        // Test search "10"
        let key10_str = pad_zero(10);
        let key10_slice = KeySlice::from(key10_str.as_bytes());
        // Assuming op_id for key "10" is 10, as per create_kv_data_in_range_zero_to
        let result10 = br.search(key10_slice, 10);
        assert_eq!(
            result10,
            Some(OpType::Write(10.to_string().as_bytes().into())),
            "Should find key '10'"
        );

        let key0_str = pad_zero(0);
        let key0_slice = KeySlice::from(key0_str.as_bytes());
        let result0 = br.search(key0_slice, 0); // op_id for key "0" is 0
        assert_eq!(
            result0,
            Some(OpType::Write(0.to_string().as_bytes().into())),
            "Should find key '0'"
        );

        // Test search "100" (not found)
        let key100_str = pad_zero(100); // This key is not in the 0..100 range
        let key100_slice = KeySlice::from(key100_str.as_bytes());
        // Use a relevant op_id, e.g., 100, though it shouldn't matter if the key isn't present.
        let result100 = br.search(key100_slice, 100);
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
        let block_builder = BlockBuilder::new();
        let block_reader = BlockReader::new(
            block_builder
                .fill(&mut kvs.iter().peekable())
                .unwrap()
                .into_inner(),
        );

        // Check search result for key "5" with a high enough op_id
        let f = 5.to_string();
        let search_key = KeySlice::from(f.as_bytes());
        let search_op_id = 20; // Ensure all versions are considered
        let result = block_reader.search(search_key, search_op_id);

        // Assert that the latest version ("duplicate_15") is found
        assert_eq!(
            result,
            Some(OpType::Write("duplicate_15".as_bytes().into()))
        );
    }

    #[test]
    fn test_block_builder() {
        // write kv, key range from string 0 to string 100 ,value is same as key, to BlockBuilder
        let iter = create_kv_data_in_range_zero_to(100);
        let br = build_block_from_kvs_iter(&iter);
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
        let br = build_block_from_kvs_iter(&kvs);

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
        let br = build_block_from_kvs_iter(&kvs);

        // The first key should be the key from the first KV operation
        let expected_first_key = &kvs[0].key;

        // Assert that the first_key method returns the correct first key
        assert_eq!(
            br.first_key(),
            expected_first_key,
            "BlockReader first_key should match the first key used to build it"
        );
    }

    #[test]
    fn test_block_iter() {
        let iter = create_kv_data_in_range_zero_to(100);
        let mut kv_iter = iter.iter();
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();
        let block_builder = BlockBuilder::new();
        let br = BlockReader::new(block_builder.fill(&mut kv_iter_agg).unwrap().into_inner());
        let block_iter = br.to_iter(); // Pass ownership
        let mut count = 0;
        for (i, kv) in block_iter.enumerate() {
            assert_eq!(kv.key, pad_zero(i as u64).as_bytes().into());
            count += 1;
        }
        assert_eq!(count, 100);
    }

    #[test]
    fn test_block_reader_new() {
        // Create some KV operations
        let kvs = create_kv_data_in_range_zero_to(5); // Use 5 KVs

        // Build a block from these KVs
        let mut kv_iter = kvs.iter();
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();
        let block_builder = BlockBuilder::new();
        let buffer = block_builder.fill(&mut kv_iter_agg).expect("Failed to build block");
        let block_data = buffer.into_inner();

        // Create a BlockReader from the raw block data
        let block_reader = BlockReader::new(block_data);

        // Assert that the count and first key are correctly read
        assert_eq!(block_reader.count(), 5, "BlockReader should have 5 items");
        assert_eq!(block_reader.first_key(), &kvs[0].key, "BlockReader first key should match the first KV's key");
    }


    #[test]
    fn test_block_builder_empty_iterator() {
        // Create an empty iterator
        let kvs: Vec<KVOpertion> = Vec::new();
        let mut kv_iter = kvs.iter();
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();

        // Build a block with the empty iterator - should result in an error
        let block_builder = BlockBuilder::new();
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
        let mut kv_iter = kvs.iter();
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();

        // Build a block with the oversized data
        let block_builder = BlockBuilder::new();
        let br = BlockReader::new(block_builder.fill(&mut kv_iter_agg).unwrap().into_inner());

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

        // Verify the block's size is within DATA_BLOCK_SIZE
        let block_size = br.data.borrow().position() as usize;
        assert!(
            block_size <= DATA_BLOCK_SIZE,
            "Block size ({}) should not exceed DATA_BLOCK_SIZE ({})",
            block_size,
            DATA_BLOCK_SIZE
        );

        // Verify the first key matches
        assert_eq!(br.first_key(), &kvs[0].key, "First key should match");

        // Verify we can iterate through all items in the block
        let br_count = br.count();
        let mut block_iter = br.to_iter();
        let mut count = 0;
        while let Some(_) = block_iter.next() {
            count += 1;
        }
        assert_eq!(count, br_count, "Iterator count should match block count");
    }

}
