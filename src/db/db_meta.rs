use std::{
    io::Write,
    sync::{Arc, Mutex},
};

use super::store::{Store, StoreId};
use crate::db::common::OpId;

const META_FILE_NAME: &str = "db_meta";
pub struct DBMeta<T: Store> {
    meta_data_store: T,
    next_key_id: OpId, //read from logfile so dont need to save to file when update this
    next_sstable_id: StoreId, // dont need to save to file
    // vec[0]= level 0
    // need save to store when update
    sstables: Vec<Vec<StoreId>>,
}

pub type ThreadDbMeta<T> = Arc<Mutex<DBMeta<T>>>;
impl<T: Store> DBMeta<T> {
    // create new db meta
    pub fn new(s: T) -> Self {
        unimplemented!()
    }
    // open from exit db meta
    pub fn open(s: T) -> Self {
        unimplemented!()
    }
    // [opid,opid+size) is useable for caller
    pub fn increase_key_ids(&mut self, size: usize) -> OpId {
        unimplemented!()
    }
    pub fn increase_sstable_id(&mut self, size: usize) -> StoreId {
        unimplemented!()
    }

    // sync meta date to store
    pub fn sync(&mut self) {}

    // append current meta to meta data store
    fn append_meta(&mut self, data: &[u8]) {
        unimplemented!()
    }
    fn encode(&self) -> Vec<u8> {
        unimplemented!()
    }
    fn decode(data: &[u8]) -> Self {
        unimplemented!()
    }
}
