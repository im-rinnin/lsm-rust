use std::iter::Peekable;
use std::ops::Not;
use std::usize;

use serde::{Deserialize, Serialize};

use crate::db::common::{
    kv_opertion_len, read_kv_operion, write_kv_operion, Buffer, KVOpertion, KVOpertionRef,
    KViterAgg, Key,
};

pub const DATA_BLOCK_SIZE: usize = 4 * 1024;
pub struct BlockIter<'a> {
    buffer: &'a mut Buffer,
    count: usize,
    current: usize,
}
impl<'a> Iterator for BlockIter<'a> {
    type Item = KVOpertion;
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.count {
            return None;
        }
        let res = read_kv_operion(self.buffer);
        self.current += 1;
        return Some(res);
    }
}
impl<'a> BlockIter<'a> {
    pub fn new(buffer: &'a mut Buffer, count: usize) -> Self {
        buffer.set_position(0);
        BlockIter {
            buffer,
            count,
            current: 0,
        }
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
    for item in &mut *it {
        let size = kv_opertion_len(&item);
        if size + w.position() as usize >= DATA_BLOCK_SIZE {
            break;
        }
        last_position = w.position();
        write_kv_operion(&item, w);
        len += 1;
    }
    let position = w.position();
    w.set_position(last_position);
    let last = read_kv_operion(w).key;
    w.set_position(position);
    assert!(w.position() < DATA_BLOCK_SIZE as u64);

    (first, last, len)
}

use super::OpId;
use super::OpType;
pub fn search_in_block(r: &mut Buffer, kv_len: usize, key: &Key, op_id: OpId) -> Option<OpType> {
    r.set_position(0);
    let mut block_iter = BlockIter::new(r, kv_len).peekable();
    while block_iter.peek().is_some() {
        let mut kv_op = block_iter.next().unwrap();
        if kv_op.id > op_id {
            continue;
        }
        // find last kv id is less or eq
        if kv_op.key.eq(key) {
            let mut res = kv_op;
            while block_iter.peek().is_some() {
                let kv_op = block_iter.next().unwrap();
                if kv_op.id <= op_id && kv_op.key.eq(key) {
                    res = kv_op;
                } else {
                    break;
                }
            }
            return Some(res.op);
        }
    }
    None
}

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
    use crate::db::sstable::block::search_in_block;
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
    fn test_search_in_block() {
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
        let mut block = Cursor::new(Vec::new());
        for kv in kvs_ref {
            write_kv_operion(&kv, &mut block);
        }
        let res = search_in_block(&mut block, count, &0.to_string(), 0).unwrap();
        let expect = OpType::Write(0.to_string());
        assert_eq!(res, expect);

        let res = search_in_block(&mut block, count, &50.to_string(), 50).unwrap();
        let expect = OpType::Write(50.to_string());
        assert_eq!(res, expect);

        let res = search_in_block(&mut block, count, &100.to_string(), 50);
        assert!(res.is_none());

        let res = search_in_block(&mut block, count, &5.to_string(), 1);
        assert!(res.is_none());

        let res = search_in_block(&mut block, count, &99.to_string(), 100).unwrap();
        let expect = OpType::Delete;
        assert_eq!(res, expect);
        let res = search_in_block(&mut block, count, &99.to_string(), 105).unwrap();
        let expect = OpType::Write(1.to_string());
        assert_eq!(res, expect);
    }
    #[test]
    fn test_block_iter() {
        let iter = create_data_source_for_test(100);
        let mut kv_iter = iter.iter().map(|kv| KVOpertionRef::new(kv));
        let mut kv_iter_agg = KViterAgg::new(vec![&mut kv_iter]).peekable();
        let mut buffer = new_buffer(DATA_BLOCK_SIZE);
        fill_block(&mut buffer, &mut kv_iter_agg);
        buffer.set_position(0);
        let block_iter = BlockIter::new(&mut buffer, 100);
        let mut count = 0;
        for (i, kv) in block_iter.enumerate() {
            assert_eq!(kv.key, i.to_string());
            count += 1;
        }
        assert_eq!(count, 100);
    }
}
