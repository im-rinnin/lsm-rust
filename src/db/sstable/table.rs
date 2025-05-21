// table 结构
// [datablock(4kb)] * n  总大小2mb  每个block，4kb对齐
// [datablockmeta]*n 总大小不受限制 描述每个block 的meta 4kb 对齐
// table meta 最后一个block table的Meta置 4kb
use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::binary_heap::PeekMut,
    io::{Cursor, Read, Seek, Write},
    iter::Peekable,
    ops::Not,
    u64, usize,
};

use bincode::{config::LittleEndian, Options};
use serde::{Deserialize, Serialize};

use crate::db::{
    common::{
        kv_opertion_len, new_buffer, Buffer, KVOpertion, KViterAgg, OpId, OpType, Result, Value,
    },
    key::{KeySlice, KeyVec},
    memtable::Memtable,
    sstable::block::{fill_block, next_block_start_postion, write_block_metas, DataBlockMeta},
    store::{Store, StoreId},
};

use super::block::{block_data_start_offset, read_block_meta, BlockIter, DATA_BLOCK_SIZE};
pub type SStableId = u64;
const SSTABLE_DATA_SIZE_LIMIT: usize = 2 * 1024 * 1024;
const BLOCK_COUNT_LIMIT: usize = SSTABLE_DATA_SIZE_LIMIT / DATA_BLOCK_SIZE;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct TableMeta {
    last_key: KeyVec,
    data_block_count: u64,
    meta_block_count: u64,
}

struct TableIter<'a, T: Store> {
    table: &'a TableReader<T>,
    table_meta: TableMeta,
    block_metas: Vec<DataBlockMeta>,
    current_block_num: usize,
}
struct SStableBlock {
    data: Vec<KVOpertion>,
    meta: DataBlockMeta,
}

pub struct TableReader<T: Store> {
    store: T,
}

impl<'a, T: Store> TableIter<'a, T> {
    fn new(table: &'a TableReader<T>) -> Self {
        let table_meta = table.read_table_meta();
        let metas = table.read_block_meta(&table_meta);
        TableIter {
            table,
            table_meta,
            block_metas: metas,
            current_block_num: 0,
        }
    }
}
impl<'a, T: Store> Iterator for TableIter<'a, T> {
    type Item = SStableBlock; // The iterator yields SStableBlock

    fn next(&mut self) -> Option<Self::Item> {
        // Check if we have processed all data blocks according to the table metadata
        if self.current_block_num >= self.table_meta.data_block_count as usize {
            return None; // Iteration finished
        }

        // Calculate the file offset for the current data block
        let offset = self.current_block_num * DATA_BLOCK_SIZE;

        // Create a new buffer for this block's data
        let mut block_buffer = new_buffer(DATA_BLOCK_SIZE);

        // Read exactly DATA_BLOCK_SIZE bytes from the store into the new buffer's Vec
        self.table.store.read_at(block_buffer.get_mut(), offset);

        // Reset the buffer's position so the consumer can read from the start
        block_buffer.set_position(0);

        // Get the metadata for the current block
        let block_meta = self.block_metas[self.current_block_num].clone(); // Assuming DataBlockMeta is Clone

        // Create a BlockIter to read KVOpertions from the buffer
        let block_iter = BlockIter::new(block_buffer, block_meta.count); // Pass ownership

        // Collect all KVOpertions into a Vec
        let data: Vec<KVOpertion> = block_iter.collect();

        // Increment the block number for the *next* call to next()
        self.current_block_num += 1;

        // Return the SStableBlock containing the data and metadata
        Some(SStableBlock {
            data,
            meta: block_meta,
        })
    }
}

fn create_table<'a, T: Store>(store: &mut T, it: &mut Peekable<KViterAgg<'a>>) {
    assert!(it.peek().is_some());
    let mut block_buffer = new_buffer(DATA_BLOCK_SIZE);
    let mut metas = Vec::<DataBlockMeta>::new();
    let mut block_count = 0;
    //  check it next,if is none write block meta, table meta, return
    // write kv to block
    while it.peek().is_some() && block_count < BLOCK_COUNT_LIMIT {
        // reset buffer and fill with it
        block_buffer.set_position(0);
        let (first_key, last_key, count) = fill_block(&mut block_buffer, it);
        // write to store
        store.seek(block_count * DATA_BLOCK_SIZE);
        append_buffer_to_store(&block_buffer, store);
        block_count += 1;
        let meta = DataBlockMeta {
            last_key,
            count: count,
        };
        metas.push(meta);
    }
    // save block meta to store
    store.seek(next_block_start_postion(store.len()));
    assert_eq!(store.len() % DATA_BLOCK_SIZE, 0);
    block_buffer.set_position(0);
    write_block_metas(&metas, &mut block_buffer);

    let mut meta_block_count = block_buffer.position() / (DATA_BLOCK_SIZE as u64);
    let a = if block_buffer.position() % (DATA_BLOCK_SIZE as u64) == 0 {
        0
    } else {
        1
    };
    meta_block_count += a;

    append_buffer_to_store(&block_buffer, store);
    // store seek to next block
    store.seek(next_block_start_postion(store.len()));
    // save table meta to store

    assert_eq!(store.len() % DATA_BLOCK_SIZE, 0);
    let table_last_key = metas.last().unwrap().last_key.clone();

    block_buffer.set_position(0);
    write_table_meta(
        &TableMeta {
            last_key: table_last_key,
            data_block_count: block_count as u64,
            meta_block_count,
        },
        &mut block_buffer,
    );
    append_buffer_to_store(&block_buffer, store);

    // flush store
    store.flush();
}

// write to last block
fn append_buffer_to_store<T: Store>(buffer: &Buffer, store: &mut T) {
    let v = buffer.get_ref();
    store.append(&v[0..buffer.position() as usize]);
}

// for level 0,must consider multiple key with diff value/or delete
impl<T: Store> TableReader<T> {
    fn new(mut store: T) -> Self {
        TableReader { store }
    }
    fn read_table_meta(&self) -> TableMeta {
        let meta_postion = (self.store.len() / DATA_BLOCK_SIZE) * DATA_BLOCK_SIZE;
        let mut data = vec![0; DATA_BLOCK_SIZE];
        self.store.read_at(&mut data.as_mut_slice(), meta_postion);
        let mut block = Cursor::new(data);
        read_table_meta(&mut block)
    }
    fn read_block_meta(&self, table_meta: &TableMeta) -> Vec<DataBlockMeta> {
        let offset = block_data_start_offset(table_meta.data_block_count as usize);
        let mut buffer = new_buffer((table_meta.meta_block_count as usize) * DATA_BLOCK_SIZE);
        self.store.read_at(buffer.get_mut().as_mut_slice(), offset);
        let metas = read_block_meta(&mut buffer, table_meta.data_block_count as usize);
        metas
    }
    fn to_iter(&self) -> TableIter<T> {
        TableIter::new(self)
    }

    fn find(&self, key: KeySlice, id: OpId) -> Option<OpType> {
        // check table last_key return none if key is greater
        let table_meta = self.read_table_meta();
        if table_meta.last_key.as_ref().lt(key.as_ref()) {
            return None;
        }
        let metas = self.read_block_meta(&table_meta);
        // find block which is the first block whose last_key is greater or eq key
        for (i, meta) in metas.iter().enumerate() {
            if meta.last_key.as_ref().ge(key.as_ref()) {
                let mut buffer = new_buffer(DATA_BLOCK_SIZE);
                let mut v = buffer.get_mut().as_mut_slice();
                // Read the block data
                self.store.read_at(&mut v, DATA_BLOCK_SIZE * i);
                // Create an iterator for the block and search within it
                let mut block_iter = BlockIter::new(buffer, meta.count as usize);
                let res = block_iter.search(key, id);
                return res;
            }
        }
        None
    }
}

fn write_table_meta(meta: &TableMeta, w: &mut Buffer) {
    assert_eq!(w.position(), 0);
    bincode::serialize_into(&mut *w, &meta);
    assert!((w.position() as usize) < DATA_BLOCK_SIZE);
}
fn read_table_meta(r: &mut Buffer) -> TableMeta {
    assert_eq!(r.position(), 0);
    bincode::deserialize_from(r).unwrap()
}

#[cfg(test)]
pub mod test {
    use bincode::serialize_into;

    use super::{TableMeta, DATA_BLOCK_SIZE};
    use core::panic;
    use std::{
        hash::BuildHasher, io::Cursor, iter::Peekable, ops::Range, os::unix::fs::MetadataExt,
        process::Output, rc::Rc, str::FromStr, usize,
    };

    use super::super::block::test::pad_zero;
    use crate::db::{
        common::{kv_opertion_len, KVOpertion, OpType},
        sstable::{
            self,
            block::{
                read_block_meta, test::create_kv_data_with_range, write_block_metas, BlockIter,
                DataBlockMeta,
            },
            table::{
                create_table, fill_block, next_block_start_postion, read_table_meta,
                write_table_meta,
            },
        },
        store::{Memstore, Store},
    };

    use super::super::block::test::create_kv_data_for_test;
    use super::{KViterAgg, TableReader};
    pub fn create_test_table(range: Range<usize>) -> TableReader<Memstore> {
        let v = create_kv_data_with_range(range);
        let id = "1".to_string();
        let mut store = Memstore::new(&id);
        let mut it = v.iter();
        let mut iter = KViterAgg::new(vec![&mut it]).peekable();
        create_table(&mut store, &mut iter);
        let table: TableReader<Memstore> = TableReader::new(store);
        table
    }
    fn create_test_table_with_size(size: usize) -> TableReader<Memstore> {
        create_test_table(0..size)
    }

    #[test]
    fn test_table_build_use_large_iter_and_check_by_iterator() {
        let num = 70000;
        //test data more than table
        let mut kvs = create_kv_data_for_test(num);

        let mut kvs_ref = kvs.iter();
        let mut iter = KViterAgg::new(vec![&mut kvs_ref]).peekable();
        let id = "1".to_string();
        let mut store = Memstore::new(&id);
        create_table(&mut store, &mut iter);

        let table = TableReader::new(store);
        let meta = table.read_table_meta();
        let lasy_key = meta.last_key;

        let table_iter = table.to_iter();
        let mut kv_index = 0; // Track index in the original kvs Vec

        for sstable_block in table_iter {
            for kv in sstable_block.data {
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
    #[test]
    fn test_table_meta_read_write() {
        let meta = TableMeta {
            last_key: "last".to_string().as_bytes().into(),
            data_block_count: 1,
            meta_block_count: 1,
        };
        let mut block = Cursor::new(Vec::new());
        write_table_meta(&meta, &mut block);
        block.set_position(0);
        let res = read_table_meta(&mut block);
        assert_eq!(res, meta);
    }

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
    }
}
