use super::{store::*};
use crate::db::common::KVOpertion;
const LOG_FILE_NAME: &str = "logfile";
pub struct LogFile<T: Store> {
    s: T,
}

impl<T: Store> LogFile<T> {
    pub fn new(store: T) -> Self {
        unimplemented!()
    }
    pub fn from(store_id: StoreId, store: T) -> Self {
        unimplemented!()
    }
    pub fn append(&mut self, op: KVOpertion) {
        unimplemented!()
    }
}
