use std::iter::Peekable;
use std::ops::Not;
use std::usize;

use crate::db::common::OpId;
use crate::db::common::OpType;
use crate::db::key::KeySlice;
use byteorder::WriteBytesExt;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Read, Write};

use crate::db::common::{kv_opertion_len, Buffer, KVOpertion, KViterAgg}; // Removed write_kv_operion

pub const DATA_BLOCK_SIZE: usize = 4 * 1024;
// may contains same key
pub struct BlockIter {
    buffer: Buffer, // Owns the buffer now
    count: usize,
    current: usize,
}
use crate::db::key::KeyVec;
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct DataBlockMeta {
    pub last_key: KeyVec,
    pub count: usize,
}

impl Iterator for BlockIter {
    type Item = KVOpertion;
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.count {
            return None;
        }
        let res = read_kv_operion(&mut self.buffer);
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

    // Searches for the latest operation for a given key with an OpId <= the provided op_id.
    // This method consumes the iterator up to the point where the key is found or passed.
    // deduplicate kv, return kv with max id
    pub fn search(&mut self, key: KeySlice, op_id: OpId) -> Option<OpType> {
        let mut result: Option<OpType> = None;
        let mut last_matching_id: OpId = 0; // Keep track of the highest ID found for the key <= op_id

        while let Some(kv_op) = self.next() {
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
}

pub fn read_kv_operion(r: &mut Cursor<Vec<u8>>) -> KVOpertion {
    let id = r.read_u64::<LittleEndian>().unwrap();
    let key_len = r.read_u64::<LittleEndian>().unwrap();
    let mut tmp = vec![0; key_len as usize];
    r.read_exact(&mut tmp);
    let key = KeyVec::from_vec(tmp);
    let op_type = r.read_u8().unwrap();
    let op = match op_type {
        0 => OpType::Delete,
        _ => {
            let value_len = r.read_u64::<LittleEndian>().unwrap();
            let mut tmp = vec![0; value_len as usize];
            r.read_exact(&mut tmp);
            let value = String::from_utf8(tmp).unwrap();
            OpType::Write(value.as_bytes().into())
        }
    };
    KVOpertion { id, key, op }
}

// Moved from common.rs
pub fn write_kv_operion(kv_opertion: &KVOpertion, w: &mut dyn Write) {
    w.write_u64::<LittleEndian>(kv_opertion.id);
    let key_len = kv_opertion.key.len() as u64;
    w.write_u64::<LittleEndian>(key_len);
    w.write(kv_opertion.key.as_ref());
    match &kv_opertion.op {
        OpType::Delete => {
            w.write_u8(0);
        }
        OpType::Write(v) => {
            w.write_u8(1);
            let v_len = v.len() as u64;
            w.write_u64::<LittleEndian>(v_len);
            w.write(v.as_ref());
        }
    }
}

pub fn block_data_start_offset(block_count: usize) -> usize {
    block_count * DATA_BLOCK_SIZE
}

pub fn next_block_start_postion(current_postion: usize) -> usize {
    (current_postion / DATA_BLOCK_SIZE + 1) * DATA_BLOCK_SIZE
}

// fill block with kv from it until block reach size limit or it end
// return first_key last_key keylen
// it has at least one item
pub fn fill_block(w: &mut Buffer, it: &mut Peekable<KViterAgg>) -> (KeyVec, KeyVec, usize) {
    assert!(it.peek().is_some());
    assert!(kv_opertion_len(it.peek().unwrap()) < DATA_BLOCK_SIZE);

    let first = it.peek().unwrap().key.clone();
    let mut len = 0;
    let mut last_position = 0;
    while let Some(item) = it.peek() {
        let size = kv_opertion_len(&item);
        if size + w.position() as usize >= DATA_BLOCK_SIZE {
            break;
        }
        last_position = w.position();
        write_kv_operion(&item, w);
        len += 1;
        it.next();
    }
    let position = w.position();
    w.set_position(last_position);
    let last = read_kv_operion(w).key;
    w.set_position(position);
    assert!(w.position() < DATA_BLOCK_SIZE as u64);

    (first.as_ref().into(), last, len)
}

pub fn write_block_metas(metas: &Vec<DataBlockMeta>, w: &mut Buffer) {
    assert_eq!(w.position(), 0);
    for meta in metas {
        bincode::serialize_into(&mut *w, &meta);
    }
}
pub fn read_block_meta(r: &mut Buffer, count: usize) -> Vec<DataBlockMeta> {
    assert_eq!(r.position(), 0);
    let mut res = Vec::new();
    for i in 0..count {
        res.push(bincode::deserialize_from(&mut *r).unwrap());
    }
    res
}
#[cfg(test)]
pub mod test {
    use std::io::Cursor;
    use std::ops::Range;

    use super::read_kv_operion;
    use super::write_kv_operion;
    use crate::db::common::kv_opertion_len;
    use crate::db::common::new_buffer;
    use crate::db::common::KViterAgg;
    use crate::db::common::{KVOpertion, OpType};
    use crate::db::key::KeySlice;
    use crate::db::key::KeyVec;
    use crate::db::sstable::block::fill_block;
    use crate::db::sstable::block::next_block_start_postion;
    use crate::db::sstable::block::DATA_BLOCK_SIZE;

    use super::read_block_meta;
    use super::write_block_metas;
    use super::BlockIter;
    use super::DataBlockMeta;
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

    // Moved from common.rs test module
    pub fn create_kv_data_for_test(size: usize) -> Vec<KVOpertion> {
        create_kv_data_with_range(0..size)
    }

    #[test]
    fn test_data_meta_read_write() {
        let meta = DataBlockMeta {
            last_key: KeyVec::from("234".as_bytes()),
            count: 10,
        };
        let mut buffer = Cursor::new(Vec::new());
        let metas = vec![meta];
        write_block_metas(&metas, &mut buffer);
        buffer.set_position(0);
        let res = read_block_meta(&mut buffer, 1);
        assert_eq!(res, metas);
    }

    #[test]
    fn test_kv_operation_serializer_deserializer() {
        let op = KVOpertion {
            id: 1,
            key: "123".to_string().as_bytes().into(),
            op: OpType::Delete,
        };
        let mut v = Vec::new();
        write_kv_operion(&op, &mut v);
        assert_eq!(v.len(), kv_opertion_len(&op));
        let mut c = Cursor::new(v);
        let op_res = read_kv_operion(&mut c);
        assert_eq!(op_res, op);

        let op = KVOpertion {
            id: 1,
            key: "123".to_string().as_bytes().into(),
            op: OpType::Write("234".as_bytes().into()),
        };
        let mut v = Vec::new();
        write_kv_operion(&op, &mut v);
        assert_eq!(v.len(), kv_opertion_len(&op));
        let mut c = Cursor::new(v);
        let op_res = read_kv_operion(&mut c);
        assert_eq!(op_res, op);
    }
    #[test]
    fn test_fill_block() {
        let kvs = create_kv_data_for_test(2048);
        let mut kv_iter = kvs.iter();
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();
        let mut block = new_buffer(0);
        let (first, last, len) = fill_block(&mut block, &mut kv_iter_agg);
        assert_eq!(first, pad_zero(0).as_bytes().into());
        assert_eq!(last, pad_zero(122).as_bytes().into());
        assert_eq!(len, 123);
        assert!(block.position() < DATA_BLOCK_SIZE as u64);
        assert!(kv_iter_agg.peek().is_some());

        let kvs = create_kv_data_for_test(10);
        let mut kv_iter = kvs.iter();
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();
        let mut block = new_buffer(0);
        let (first, last, len) = fill_block(&mut block, &mut kv_iter_agg);
        assert_eq!(first, pad_zero(0).as_bytes().into());
        assert_eq!(last, pad_zero(9).as_bytes().into());
        assert_eq!(len, 10);

        assert!(block.position() < DATA_BLOCK_SIZE as u64);
        assert!(kv_iter_agg.peek().is_none());
    }
    #[test]
    fn text_next_block_postion() {
        assert_eq!(next_block_start_postion(1), DATA_BLOCK_SIZE);
        assert_eq!(
            next_block_start_postion(DATA_BLOCK_SIZE + 10),
            (DATA_BLOCK_SIZE * 2)
        );
        assert_eq!(
            next_block_start_postion(5 * DATA_BLOCK_SIZE + 10),
            (DATA_BLOCK_SIZE * 6)
        );
    }
    #[test]
    fn test_block_iter_search_with_same_key() {
        // Create initial kv data list [0..10)
        let mut kvs = create_kv_data_for_test(10);

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
        let mut buffer = new_buffer(DATA_BLOCK_SIZE);
        for kv in kvs.iter() {
            write_kv_operion(&kv, &mut buffer);
        }

        // Create BlockIter
        let mut iter = BlockIter::new(buffer.clone(), count);

        // Check search result for key "5" with a high enough op_id
        let f = 5.to_string();
        let search_key = KeySlice::from(f.as_bytes());
        let search_op_id = 20; // Ensure all versions are considered
        let result = iter.search(search_key, search_op_id);

        // Assert that the latest version ("duplicate_15") is found
        assert_eq!(
            result,
            Some(OpType::Write("duplicate_15".as_bytes().into()))
        );
    }
    #[test]
    fn test_block_iter_search() {
        // build block (0..100)
        let mut count = 100;
        let mut kvs = create_kv_data_for_test(count);
        kvs.push(KVOpertion::new(
            100,
            pad_zero(99).as_bytes().into(),
            OpType::Delete,
        ));
        kvs.push(KVOpertion::new(
            101,
            pad_zero(99).as_bytes().into(),
            OpType::Write(1.to_string().as_bytes().into()),
        ));
        count += 2;
        let kvs_ref = kvs.iter();
        let mut buffer = new_buffer(DATA_BLOCK_SIZE); // Use new_buffer for consistency
        for kv in kvs_ref {
            write_kv_operion(&kv, &mut buffer);
        }

        // Test searching for key "0" with op_id 0
        let mut iter0 = BlockIter::new(buffer.clone(), count); // Clone buffer for each search

        let res0 = iter0
            .search(KeySlice::from(pad_zero(0).as_ref()), 0)
            .unwrap();
        let expect0 = OpType::Write(0.to_string().as_bytes().into());
        assert_eq!(res0, expect0);

        // Test searching for key "50" with op_id 50
        let mut iter50 = BlockIter::new(buffer.clone(), count);
        let res50 = iter50
            .search(KeySlice::from(pad_zero(50).as_bytes()), 50)
            .unwrap();
        let expect50 = OpType::Write(50.to_string().as_bytes().into());
        assert_eq!(res50, expect50);

        // Test searching for key "100" (doesn't exist) with op_id 50
        let mut iter100 = BlockIter::new(buffer.clone(), count);
        let res100 = iter100.search(KeySlice::from(pad_zero(100).as_bytes()), 50);
        assert!(res100.is_none());

        // Test searching for key "5" with op_id 1 (should not find because op_id 5 > 1)
        let mut iter5_low_id = BlockIter::new(buffer.clone(), count);
        let res5_low_id = iter5_low_id.search(KeySlice::from(pad_zero(5).as_bytes()), 1);
        assert!(res5_low_id.is_none());

        // Test searching for key "99" with op_id 100 (should find the Delete operation)
        let mut iter99_100 = BlockIter::new(buffer.clone(), count);
        let res99_100 = iter99_100
            .search(KeySlice::from(pad_zero(99).as_bytes()), 100)
            .unwrap();
        let expect99_100 = OpType::Delete;
        assert_eq!(res99_100, expect99_100);

        // Test searching for key "99" with op_id 105 (should find the Write operation with id 101)
        let mut iter99_105 = BlockIter::new(buffer.clone(), count);
        let res99_105 = iter99_105
            .search(KeySlice::from(pad_zero(99).as_bytes()), 105)
            .unwrap();
        let expect99_105 = OpType::Write(1.to_string().as_bytes().into());
        assert_eq!(res99_105, expect99_105);
    }
    #[test]
    fn test_block_iter() {
        let iter = create_kv_data_for_test(100);
        let mut kv_iter = iter.iter();
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();
        let mut buffer = new_buffer(DATA_BLOCK_SIZE);
        fill_block(&mut buffer, &mut kv_iter_agg);
        buffer.set_position(0);
        let block_iter = BlockIter::new(buffer, 100); // Pass ownership
        let mut count = 0;
        for (i, kv) in block_iter.enumerate() {
            assert_eq!(kv.key, pad_zero(i as u64).as_bytes().into());
            count += 1;
        }
        assert_eq!(count, 100);
    }
}
