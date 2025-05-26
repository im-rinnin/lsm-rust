use super::{
    common::{KVOpertion, OpId},
    key::{KeySlice, KeyVec},
    store::{Store, StoreId},
    table::*,
};
struct StoreCreator<T: Store> {
    t: T,
}
impl<T: Store> StoreCreator<T> {
    fn new_store(&mut self) -> T {
        todo!()
    }
}
pub struct Level<T: Store> {
    is_level_zero: bool,
    sstable: Vec<TableReader<T>>,
    // init to zero
    next_table_index_for_compact: usize,
}
impl<T: Store> Level<T> {
    pub fn new(stores: Vec<TableReader<T>>, is_level_zero: bool) -> Level<T> {
        Level {
            is_level_zero,
            sstable: stores,
            next_table_index_for_compact: 0,
        }
    }
    fn search(&self, key: KeySlice, id: OpId) -> Option<KVOpertion> {
        // find table may contain key (by check table last key)
        // return none if not found
        // search kv in that tables, compare result, return kv with biggest id, return none if notfound
        todo!()
    }
}

fn compact<T: Store>(sc: StoreCreator<T>, level_low: &Level<T>, level_high: &Level<T>) -> Level<T> {
    todo!()
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
