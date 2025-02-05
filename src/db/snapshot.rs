use std::sync::Arc;

use super::{common::*, sstable::SStable, store::Store, Levels};
pub struct Snapshot<T: Store> {
    sstables: Levels<T>,
    key_id: OpId,
}
impl<T: Store> Snapshot<T> {
    pub fn new(key_id: OpId, sstables: Vec<Vec<Arc<SStable<T>>>>) -> Self {
        unimplemented!()
    }
    pub fn query(&self, q: KeyQuery) -> Result<Option<Value>> {
        unimplemented!()
    }
    pub fn range_query(&self, start: Key, end: Key) -> Result<Option<Vec<Value>>> {
        unimplemented!()
    }
}
