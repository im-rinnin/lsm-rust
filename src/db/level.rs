use std::{sync::Arc, usize};

use super::{
    common::{KVOpertion, OpId, SearchResult},
    key::{KeySlice, KeyVec},
    store::{Store, StoreId},
    table::*,
};

struct Level<T: Store> {
    // sstable sorted  by (key,op id)
    sstables: Vec<Arc<TableReader<T>>>,
}

pub struct LevelStorege<T: Store> {
    levels: Vec<Level<T>>,
    //level table increase ratio betweent two level.
    //level n+1 table len =level n table len *ratio
    level_ratio: usize,
}

impl<T: Store> LevelStorege<T> {
    pub fn new(tables: Vec<Arc<TableReader<T>>>) -> Self {
        unimplemented!()
    }
    pub fn find(&self, key: &KeySlice, opid: OpId) -> SearchResult {
        unimplemented!()
    }
    // compact table to next level tables which key range overlay
    // table=self.sstables[index]
    pub fn compact(&self, index: usize) -> Vec<TableReader<T>> {
        unimplemented!()
    }
    // return level index and table index which need to be compacted
    pub fn check_level_size(&self) -> Option<(usize, usize)> {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use std::ops::Range;

    use crate::db::table::test::create_test_table;

    use crate::db::{store::Memstore, table::TableReader};

    #[test]
    fn test_search_in_level_n() {
        // create table
        // table a key range (0,100]
        // table b range (120,200]
        // table c range (200,300]
        // create level use a,b,c
        // test search key found
        // search key 0
        // search key 300
        // search key 150
        // search key 110
        // seach not found
        // test search key 510
    }
    #[test]
    fn test_search_in_level_zero() {
        // create table
        // table a key range (0,100] value is key,id is same as key
        // table b range (60,150] value is key+1, id is key+1
        // table c range (120,200] value is key+2,id is key+2
        // create level use a,b,c
        //
        // test search key found
        // search key 0 value should be key found in a
        // search key 80 value should be key+1(181)
        // search key 180 value should be key+2(182)
        //
        // seach not found
        // test search key 110
        // test search key 220
    }
    #[test]
    fn test_compact() {}
}
