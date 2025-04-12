use super::{
    common::{KVOpertion, Key, OpId},
    sstable::table::*,
    store::Store,
    store::StoreId,
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
    sstable: Vec<TableReader<T>>,
    next_table_index_for_compact: usize,
}
impl<T: Store> Level<T> {
    pub fn new(stores: Vec<T>) -> Level<T> {
        todo!()
    }
    fn search(&self, key: &Key, id: OpId) -> Option<KVOpertion> {
        todo!()
    }
}

fn compact<T: Store>(sc: StoreCreator<T>, level_low: &Level<T>, level_high: &Level<T>) -> Level<T> {
    todo!()
}

mod test {
    use std::ops::Range;

    use crate::db::{sstable::table::TableReader, store::Memstore};

    fn create_table_reader(id_range:Range<usize>)->TableReader<Memstore>{
        todo!()

    }
    #[test]
    fn test_search(){

    }
    #[test]
    fn test_compact(){
    }
}
