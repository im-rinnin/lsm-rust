// use std::{
//     io::Write,
//     sync::{Arc, Mutex, RwLock},
// };
//
// use super::store::{Store, StoreId};
// use crate::db::common::OpId;
//
// const META_FILE_NAME: &str = "db_meta";
// pub struct DBMeta<T: Store> {
//     meta_data_store: T,
// }
//
// pub type ThreadDbMeta<T> = Arc<RwLock<DBMeta<T>>>;
//
// impl<T: Store> DBMeta<T> {
//     // create new db meta
//     pub fn new(s: T) -> Self {
//         unimplemented!()
//     }
//     // open from exit db meta
//     pub fn open(s: T) -> Self {
//         unimplemented!()
//     }
//     // [opid,opid+size) is useable for caller
//     pub fn increase_key_ids(&mut self, size: usize) -> OpId {
//         unimplemented!()
//     }
//     pub fn increase_sstable_id(&mut self, size: usize) -> StoreId {
//         unimplemented!()
//     }
//
//     // sync meta date to store
//     pub fn sync(&mut self) {}
//
//     // append current meta to meta data store
//     fn append_meta(&mut self, data: &[u8]) {
//         unimplemented!()
//     }
//     fn encode(&self) -> Vec<u8> {
//         unimplemented!()
//     }
//     fn decode(data: &[u8]) -> Self {
//         unimplemented!()
//     }
// // }
