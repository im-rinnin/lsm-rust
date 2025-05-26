// table 结构
// [<block>,<block>...<block><block_meta>...<block_meta><block_meta_offset(u64)><block_meta_count(u64)>]
// <block_meta>:[key_size,first_key,key_size,last_key]
use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::binary_heap::PeekMut,
    io::{Cursor, Read, Seek, Write},
    iter::Peekable,
    ops::Not,
    u64, usize,
};

use crate::db::{
    common::{new_buffer, Buffer, KVOpertion, KViterAgg, OpId, OpType, Result, Value},
    key::{KeySlice, KeyVec},
    memtable::Memtable,
    store::{Store, StoreId},
};
use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use serde::{Deserialize, Serialize};

use super::{
    block::{BlockIter, BlockReader, DATA_BLOCK_SIZE},
    common::{KVOpertionRef, OpTypeRef},
};
pub type SStableId = u64;
const SSTABLE_DATA_SIZE_LIMIT: usize = 2 * 1024 * 1024;
const BLOCK_COUNT_LIMIT: usize = SSTABLE_DATA_SIZE_LIMIT / DATA_BLOCK_SIZE;
#[derive(PartialEq, Debug)]
struct BlockMeta {
    first_key: KeyVec,
    last_key: KeyVec,
}

impl BlockMeta {
    pub fn encode(&self, w: &mut Buffer) {
        let first_key_len = self.first_key.len() as u64;
        w.write_u64::<LittleEndian>(first_key_len).unwrap();
        w.write_all(self.first_key.as_ref()).unwrap();

        // Encode last_key
        let last_key_len = self.last_key.len() as u64;
        w.write_u64::<LittleEndian>(last_key_len).unwrap();
        w.write_all(self.last_key.as_ref()).unwrap();
    }
    pub fn decode(r: &mut Buffer) -> Self {
        // Decode first_key
        let first_key_len = r.read_u64::<LittleEndian>().unwrap() as usize;
        let mut first_key_data = vec![0u8; first_key_len];
        r.read_exact(&mut first_key_data).unwrap();
        let first_key = KeyVec::from_vec(first_key_data);

        // Decode last_key
        let last_key_len = r.read_u64::<LittleEndian>().unwrap() as usize;
        let mut last_key_data = vec![0u8; last_key_len];
        r.read_exact(&mut last_key_data).unwrap();
        let last_key = KeyVec::from_vec(last_key_data);

        BlockMeta {
            first_key,
            last_key,
        }
    }
}

pub struct TableReader<T: Store> {
    store: T,
    block_metas: Vec<BlockMeta>,
}
// for level 0,must consider multiple key with diff value/or delete
impl<T: Store> TableReader<T> {
    fn new(mut store: T) -> Self {
        unimplemented!()
    }
    fn to_iter(&self) -> TableIter<T> {
        TableIter::new(self)
    }

    fn find(&self, key: KeySlice, id: OpId) -> Option<OpType> {
        unimplemented!()
        //     // check table last_key return none if key is greater
        //     let table_meta = self.read_table_meta();
        //     if table_meta.last_key.as_ref().lt(key.as_ref()) {
        //         return None;
        //     }
        //     let metas = self.read_block_meta(&table_meta);
        //     // find block which is the first block whose last_key is greater or eq key
        //     for (i, meta) in metas.iter().enumerate() {
        //         if meta.last_key.as_ref().ge(key.as_ref()) {
        //             let mut buffer = new_buffer(DATA_BLOCK_SIZE);
        //             let mut v = buffer.get_mut().as_mut_slice();
        //             // Read the block data
        //             self.store.read_at(&mut v, DATA_BLOCK_SIZE * i);
        //             // Create an iterator for the block and search within it
        //             let mut block_iter = BlockIter::new(buffer, meta.count as usize);
        //             let res = block_iter.search(key, id);
        //             return res;
        //         }
        //     }
        //     None
    }
}
struct TableIter<'a, T: Store> {
    table: &'a TableReader<T>,
}
impl<'a, T: Store> TableIter<'a, T> {
    fn new(table: &'a TableReader<T>) -> Self {
        unimplemented!()
        // let table_meta = table.read_table_meta();
        // let metas = table.read_block_meta(&table_meta);
        // TableIter {
        //     table,
        //     table_meta,
        //     current_block_num: 0,
        // }
    }
}
impl<'a, T: Store> Iterator for TableIter<'a, T> {
    type Item = BlockReader; // The iterator yields SStableBlock

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
    // fn next(&mut self) -> Option<Self::Item> {
    //     // Check if we have processed all data blocks according to the table metadata
    //     if self.current_block_num >= self.table_meta.data_block_count as usize {
    //         return None; // Iteration finished
    //     }
    //
    //     // Calculate the file offset for the current data block
    //     let offset = self.current_block_num * DATA_BLOCK_SIZE;
    //
    //     // Create a new buffer for this block's data
    //     let mut block_buffer = new_buffer(DATA_BLOCK_SIZE);
    //
    //     // Read exactly DATA_BLOCK_SIZE bytes from the store into the new buffer's Vec
    //     self.table.store.read_at(block_buffer.get_mut(), offset);
    //
    //     // Reset the buffer's position so the consumer can read from the start
    //     block_buffer.set_position(0);
    //
    //     // Get the metadata for the current block
    //     let block_meta = self.block_metas[self.current_block_num].clone(); // Assuming DataBlockMeta is Clone
    //
    //     // Create a BlockIter to read KVOpertions from the buffer
    //     let block_iter = BlockIter::new(block_buffer, block_meta.count); // Pass ownership
    //
    //     // Collect all KVOpertions into a Vec
    //     let data: Vec<KVOpertion> = block_iter.collect();
    //
    //     // Increment the block number for the *next* call to next()
    //     self.current_block_num += 1;
    //
    //     // Return the SStableBlock containing the data and metadata
    //     Some(SStableBlock {
    //         data,
    //         meta: block_meta,
    //     })
    // }
}

fn block_data_start_offset(block_count: usize) -> usize {
    block_count * DATA_BLOCK_SIZE
}

fn next_block_start_postion(current_postion: usize) -> usize {
    (current_postion / DATA_BLOCK_SIZE + 1) * DATA_BLOCK_SIZE
}

struct TableBuilder<T: Store> {
    store: T,
    block_metas: Vec<BlockMeta>,
    block_num_limit: usize,
    buffer: Buffer,
}
impl<T: Store> TableBuilder<T> {
    pub fn new_with_block_count(store: T, block_count: usize) -> Self {
        TableBuilder {
            store,
            block_metas: Vec::new(),
            block_num_limit: block_count,
            buffer: new_buffer(DATA_BLOCK_SIZE),
        }
    }
    pub fn new(store: T) -> Self {
        TableBuilder {
            store,
            block_metas: Vec::new(),
            block_num_limit: BLOCK_COUNT_LIMIT,
            buffer: new_buffer(DATA_BLOCK_SIZE),
        }
    }

    // implement add ai
    pub fn add(&mut self, op: KVOpertionRef) -> bool {
        // add op to current buffer
        // if buffer add fail write buffer  content to store
        // if block_num_limit return false
        // create new buffer add op to it
        unimplemented!()
    }
    pub fn flush(mut self) -> TableReader<T> {
        unimplemented!()
    }
    pub fn fill_with_op<'a, IT: Iterator<Item = &'a KVOpertion>>(&mut self, it: IT) {
        let op_ref_iter = it.map(|op| KVOpertionRef::from_op(op));
        self.fill_with_op_ref(op_ref_iter);
    }
    pub fn fill_with_op_ref<'a, IT: Iterator<Item = KVOpertionRef<'a>>>(&mut self, it: IT) {
        unimplemented!()
    }
}

#[cfg(test)]
pub mod test {
    use bincode::serialize_into;

    use super::{TableBuilder, DATA_BLOCK_SIZE};
    use core::panic;
    use std::{
        hash::BuildHasher, io::Cursor, iter::Peekable, ops::Range, os::unix::fs::MetadataExt,
        process::Output, rc::Rc, str::FromStr, usize,
    };

    use super::super::block::test::pad_zero;
    use crate::db::{
        block::{test::create_kv_data_with_range, BlockIter},
        common::{KVOpertion, OpType},
        store::{Memstore, Store},
    };

    use super::super::block::test::create_kv_data_in_range_zero_to;
    use super::{KViterAgg, TableReader};
    use crate::db::KVOpertionRef;
    pub fn create_test_table(range: Range<usize>) -> TableReader<Memstore> {
        let v = create_kv_data_with_range(range);
        let id = "1".to_string();
        let mut store = Memstore::new(&id);
        let mut it = v.iter();
        let mut table = TableBuilder::new(store);
        table.fill_with_op(it);
        let res = table.flush();
        res
    }
    fn create_test_table_with_size(size: usize) -> TableReader<Memstore> {
        create_test_table(0..size)
    }

    fn create_test_kvs_for_add_test() -> Vec<KVOpertion> {
        vec![
            KVOpertion::new(
                0,
                "keyA".as_bytes().into(),
                OpType::Write("valueA".as_bytes().into()),
            ),
            KVOpertion::new(
                1,
                "keyB".as_bytes().into(),
                OpType::Write("valueB".as_bytes().into()),
            ),
            KVOpertion::new(
                2,
                "keyC".as_bytes().into(),
                OpType::Write("valueC".as_bytes().into()),
            ),
        ]
    }

    #[test]
    fn test_table_build_add() {
        let kvs = create_test_kvs_for_add_test();

        let id = "test_table_build_add_store".to_string();
        let store = Memstore::new(&id);
        let mut tb = TableBuilder::new(store);

        for kv_op in &kvs {
            tb.add(KVOpertionRef::from_op(kv_op));
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
        for block_reader in table_iter {
            let block_iter = block_reader.to_iter();
            for kv_op_read in block_iter {
                collected_kvs.push(kv_op_read);
            }
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

        let id = "test_table_build_store".to_string();
        let store = Memstore::new(&id);
        let mut tb = TableBuilder::new(store);

        let mut kvs_iter = kvs.iter();
        tb.fill_with_op(&mut kvs_iter);
        let table_reader = tb.flush();

        // Check block metas
        assert_eq!(table_reader.block_metas.len(), 1);
        let block_meta = &table_reader.block_metas[0];
        assert_eq!(block_meta.first_key.as_ref(), "keyA".as_bytes());
        assert_eq!(block_meta.last_key.as_ref(), "keyC".as_bytes());

        // Check table contents by iterating
        let mut collected_kvs = Vec::new();
        let table_iter = table_reader.to_iter();
        for block_reader in table_iter {
            let block_iter = block_reader.to_iter();
            for kv_op in block_iter {
                collected_kvs.push(kv_op);
            }
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
        let mut kvs = create_kv_data_in_range_zero_to(num);

        let mut kvs_ref = kvs.iter();
        let id = "1".to_string();
        let mut store = Memstore::new(&id);
        let mut tb = TableBuilder::new(store);
        tb.fill_with_op(&mut kvs_ref);
        let table = tb.flush();

        let meta = &table.block_metas;
        let lasy_key = &meta.last().expect("block_metas empty").last_key;

        let table_iter = table.to_iter();
        let mut kv_index = 0; // Track index in the original kvs Vec

        for sstable_block in table_iter {
            let iter = sstable_block.to_iter();
            for kv in iter {
                // Find the corresponding original kv operation
                // This assumes the order is preserved, which should be the case
                // if KViterAgg sorts correctly.
                let original_kv = &kvs[kv_index];

                assert_eq!(kv.id, original_kv.id);
                assert_eq!(kv.key, original_kv.key);
                assert_eq!(kv.op, original_kv.op);
                kv_index += 1;
            }
        }
        // table has not enough space to save all kv
        assert!(kv_index < num);
        // check kv num
        let lasy_key_string = lasy_key.to_string();
        assert_eq!(kv_index, lasy_key_string.parse::<usize>().unwrap() + 1);
    }
    // #[test]
    // fn text_next_block_position() {
    //     assert_eq!(next_block_start_postion(1), DATA_BLOCK_SIZE);
    //     assert_eq!(
    //         next_block_start_postion(DATA_BLOCK_SIZE + 10),
    //         (DATA_BLOCK_SIZE * 2)
    //     );
    //     assert_eq!(
    //         next_block_start_postion(5 * DATA_BLOCK_SIZE + 10),
    //         (DATA_BLOCK_SIZE * 6)
    //     );
    // }

    #[test]
    fn test_table_reader() {
        let len = 100;
        let table = create_test_table_with_size(len);

        for i in 0..len {
            let res = table
                .find(pad_zero(i as u64).as_bytes().into(), i as u64)
                .expect(&format!("{} should in table", i));
            assert_eq!(res, OpType::Write(i.to_string().as_bytes().into()));
        }

        let res = table.find("100".as_bytes().into(), 0);
        assert!(res.is_none());

        // Test finding key at the beginning of the range
        let res_start = table
            .find(pad_zero(0 as u64).as_bytes().into(), 0 as u64)
            .expect("0 should in table");
        assert_eq!(res_start, OpType::Write(0.to_string().as_bytes().into()));

        // Test finding key at the end of the range
        let res_end = table
            .find(pad_zero(99 as u64).as_bytes().into(), 99 as u64)
            .expect("99 should in table");
        assert_eq!(res_end, OpType::Write(99.to_string().as_bytes().into()));

        // Test finding key in the middle of the range
        let res_middle = table
            .find(pad_zero(50 as u64).as_bytes().into(), 50 as u64)
            .expect("50 should in table");
        assert_eq!(res_middle, OpType::Write(50.to_string().as_bytes().into()));

        // Test searching for a key with a higher OpId (should still find the existing one)
        let res_higher_id = table
            .find(pad_zero(10 as u64).as_bytes().into(), 100 as u64) // Use a higher ID
            .expect("10 should still be in table with higher ID");
        assert_eq!(
            res_higher_id,
            OpType::Write(10.to_string().as_bytes().into())
        );

        // Test searching for a key below the range
        let res_below = table.find("-1".as_bytes().into(), 0);
        assert!(res_below.is_none());
    }

    #[test]
    fn test_block_meta_encode_decode() {
        use super::BlockMeta;
        use crate::db::common::new_buffer;
        use crate::db::key::KeyVec;
        use std::io::Seek;

        let original_meta = BlockMeta {
            first_key: KeyVec::from_vec(b"key_a".to_vec()),
            last_key: KeyVec::from_vec(b"key_z".to_vec()),
        };

        let mut buffer = new_buffer(1024); // Sufficiently large buffer
        original_meta.encode(&mut buffer);

        // Reset buffer position to the beginning for decoding
        buffer.seek(std::io::SeekFrom::Start(0)).unwrap();

        let decoded_meta = BlockMeta::decode(&mut buffer);

        assert_eq!(original_meta, decoded_meta);
    }
}
