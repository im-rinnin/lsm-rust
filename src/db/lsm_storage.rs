use std::sync::mpsc::Sender;
use std::sync::Arc;

use crate::db::common::*;
use crate::db::db_meta::DBMeta;
use crate::db::level::LevelStorege;
use crate::db::logfile::LogFile;
use crate::db::memtable::Memtable;
use crate::db::store::Filestore;
use crate::db::store::Memstore;
use crate::db::store::Store;
use crate::db::table::TableReader;

use super::db_meta::ThreadDbMeta;

pub struct Config {
    block_size: usize,
    sstable_size: usize,
    level_factor: usize,
    first_level_sstable_num: usize,
}

pub struct LsmStorage<T: Store> {
    m: Memtable,
    //  immutable memtable
    imm: Option<Memtable>,
    // latest level storege
    current: LevelStorege<T>,
    meta: ThreadDbMeta<T>,
}

impl<T: Store> LsmStorage<T> {
    pub fn get(&self, query: KeyQuery) -> Option<KVOpertion> {
        unimplemented!()
    }
    // return key and value in [start end)
    pub fn get_range(&self, start: KeyQuery, end: KeyQuery) -> Option<KVOpertion> {
        unimplemented!()
    }
}
