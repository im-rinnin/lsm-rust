use std::iter::Peekable;
use std::ops::Not;
use std::usize;

use serde::{Deserialize, Serialize};

use crate::db::common::{
    kv_opertion_len, read_kv_operion, write_kv_operion, Buffer, KVOpertion, KVOpertionRef,
    KViterAgg, Key,
};

pub const DATA_BLOCK_SIZE: usize = 4 * 1024;
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
    pub fn search(&mut self, key: &Key, op_id: OpId) -> Option<OpType> {
        let mut result: Option<OpType> = None;
        let mut last_matching_id: OpId = 0; // Keep track of the highest ID found for the key <= op_id

        while let Some(kv_op) = self.next() {
            if kv_op.key.eq(key) {
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct DataBlockMeta {
    pub last_key: Key,
    pub count: usize,
}

pub fn block_data_start_offset(block_count: usize) -> usize {
    block_count * DATA_BLOCK_SIZE
}

pub fn next_block_postion(current_postion: usize) -> usize {
    (current_postion / DATA_BLOCK_SIZE + 1) * DATA_BLOCK_SIZE
}

// fill block with kv from it until block reach size limit or it end
// return first_key last_key keylen
// it has at least one item
pub fn fill_block(w: &mut Buffer, it: &mut Peekable<KViterAgg>) -> (Key, Key, usize) {
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

    (first, last, len)
}
use crate::db::common::OpId;
use crate::db::common::OpType;

// false if reach block size limit
fn write_kv_to_block(kv_ref: KVOpertionRef, w: &mut Buffer) -> bool {
    let size = kv_opertion_len(&kv_ref);
    if size + w.position() as usize >= DATA_BLOCK_SIZE {
        return false;
    }
    write_kv_operion(&kv_ref, w);
    true
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
mod test {
    use std::io::Cursor;

    use crate::db::common::kv_opertion_len;
    use crate::db::common::new_buffer;
    use crate::db::common::read_kv_operion;
    use crate::db::common::test::create_data_source_for_test;
    use crate::db::common::write_kv_operion;
    use crate::db::common::KViterAgg;
    use crate::db::sstable::block::fill_block;
    use crate::db::sstable::block::next_block_postion;
    use crate::db::sstable::block::DATA_BLOCK_SIZE;

    use super::read_block_meta;
    use super::write_block_metas;
    use super::BlockIter;
    use super::DataBlockMeta;
    use super::KVOpertion;
    use super::KVOpertionRef;
    use super::OpType;
    #[test]
    fn test_data_meta_read_write() {
        let meta = DataBlockMeta {
            last_key: "234".to_string(),
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
            key: "123".to_string(),
            op: OpType::Delete,
        };
        let mut v = Vec::new();
        let op_ref = KVOpertionRef::new(&op);
        write_kv_operion(&op_ref, &mut v);
        assert_eq!(v.len(), kv_opertion_len(&op_ref));
        let mut c = Cursor::new(v);
        let op_res = read_kv_operion(&mut c);
        assert_eq!(op_res, op);

        let op = KVOpertion {
            id: 1,
            key: "123".to_string(),
            op: OpType::Write("234".to_string()),
        };
        let mut v = Vec::new();
        let op_ref = crate::db::common::KVOpertionRef::new(&op);
        write_kv_operion(&op_ref, &mut v);
        assert_eq!(v.len(), kv_opertion_len(&op_ref));
        let mut c = Cursor::new(v);
        let op_res = read_kv_operion(&mut c);
        assert_eq!(op_res, op);
    }
    #[test]
    fn test_fill_block() {
        let kvs = create_data_source_for_test(2048);
        let mut kv_iter = kvs.iter().map(|kv| KVOpertionRef::new(kv));
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();
        let mut block = new_buffer(0);
        let (first, last, len) = fill_block(&mut block, &mut kv_iter_agg);
        assert_eq!(first, "0");
        assert_eq!(last, "138");
        assert_eq!(len, 139);
        assert!(block.position() < DATA_BLOCK_SIZE as u64);
        assert!(kv_iter_agg.peek().is_some());

        let kvs = create_data_source_for_test(10);
        let mut kv_iter = kvs.iter().map(|kv| KVOpertionRef::new(kv));
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();
        let mut block = new_buffer(0);
        let (first, last, len) = fill_block(&mut block, &mut kv_iter_agg);
        assert_eq!(first, "0");
        assert_eq!(last, "9");
        assert_eq!(len, 10);

        assert!(block.position() < DATA_BLOCK_SIZE as u64);
        assert!(kv_iter_agg.peek().is_none());
    }
    #[test]
    fn text_next_block_postion() {
        assert_eq!(next_block_postion(1), DATA_BLOCK_SIZE);
        assert_eq!(
            next_block_postion(DATA_BLOCK_SIZE + 10),
            (DATA_BLOCK_SIZE * 2)
        );
        assert_eq!(
            next_block_postion(5 * DATA_BLOCK_SIZE + 10),
            (DATA_BLOCK_SIZE * 6)
        );
    }
    #[test]
    fn test_block_iter_search() {
        // build block (0..100)
        let mut count = 100;
        let mut kvs = create_data_source_for_test(count);
        kvs.push(KVOpertion::new(100, 99.to_string(), OpType::Delete));
        kvs.push(KVOpertion::new(
            101,
            99.to_string(),
            OpType::Write(1.to_string()),
        ));
        count += 2;
        let kvs_ref = kvs.iter().map(|kv| KVOpertionRef::new(&kv));
        let mut buffer = new_buffer(DATA_BLOCK_SIZE); // Use new_buffer for consistency
        for kv in kvs_ref {
            write_kv_operion(&kv, &mut buffer);
        }

        // Test searching for key "0" with op_id 0
        let mut iter0 = BlockIter::new(buffer.clone(), count); // Clone buffer for each search
        let res0 = iter0.search(&0.to_string(), 0).unwrap();
        let expect0 = OpType::Write(0.to_string());
        assert_eq!(res0, expect0);

        // Test searching for key "50" with op_id 50
        let mut iter50 = BlockIter::new(buffer.clone(), count);
        let res50 = iter50.search(&50.to_string(), 50).unwrap();
        let expect50 = OpType::Write(50.to_string());
        assert_eq!(res50, expect50);

        // Test searching for key "100" (doesn't exist) with op_id 50
        let mut iter100 = BlockIter::new(buffer.clone(), count);
        let res100 = iter100.search(&100.to_string(), 50);
        assert!(res100.is_none());

        // Test searching for key "5" with op_id 1 (should not find because op_id 5 > 1)
        let mut iter5_low_id = BlockIter::new(buffer.clone(), count);
        let res5_low_id = iter5_low_id.search(&5.to_string(), 1);
        assert!(res5_low_id.is_none());

        // Test searching for key "99" with op_id 100 (should find the Delete operation)
        let mut iter99_100 = BlockIter::new(buffer.clone(), count);
        let res99_100 = iter99_100.search(&99.to_string(), 100).unwrap();
        let expect99_100 = OpType::Delete;
        assert_eq!(res99_100, expect99_100);

        // Test searching for key "99" with op_id 105 (should find the Write operation with id 101)
        let mut iter99_105 = BlockIter::new(buffer.clone(), count);
        let res99_105 = iter99_105.search(&99.to_string(), 105).unwrap();
        let expect99_105 = OpType::Write(1.to_string());
        assert_eq!(res99_105, expect99_105);
    }
    #[test]
    fn test_block_iter() {
        let iter = create_data_source_for_test(100);
        let mut kv_iter = iter.iter().map(|kv| KVOpertionRef::new(kv));
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();
        let mut buffer = new_buffer(DATA_BLOCK_SIZE);
        fill_block(&mut buffer, &mut kv_iter_agg);
        buffer.set_position(0);
        let block_iter = BlockIter::new(buffer, 100); // Pass ownership
        let mut count = 0;
        for (i, kv) in block_iter.enumerate() {
            assert_eq!(kv.key, i.to_string());
            count += 1;
        }
        assert_eq!(count, 100);
    }
}
