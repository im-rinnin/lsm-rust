use std::sync::Arc;
use super::sstable::table::TableReader;

use super::{common::*,  store::Store, level::Level};
pub struct Snapshot<T: Store> {
    levels: Vec<Level<T>>,
    key_id: OpId,
}
impl<T: Store> Snapshot<T> {
    pub fn new(key_id: OpId, sstables: Vec<Vec<Arc<TableReader<T>>>>) -> Self {
        unimplemented!()
    }
    pub fn query(&self, q: KeyQuery) -> Result<Option<Value>> {
        unimplemented!()
    }
    pub fn range_query(&self, start: Key, end: Key) -> Result<Option<Vec<Value>>> {
        unimplemented!()
    }
}
