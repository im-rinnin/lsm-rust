use super::store::Store;
use super::table::SStableId;
use super::OpId;

const META_FILE_NAME: &str = "db_meta";
pub struct DBMeta<T: Store> {
    meta_data_store: T,
    next_key_id: OpId, //read from logfile so dont need to save to file when update this
    next_sstable_id: SStableId, // dont need to save to file
    // vec[0]= level 0
    // need save to store when update
    sstables: Vec<Vec<SStableId>>,
}

impl<T: Store> DBMeta<T> {
    fn new(s: T) -> Self {
        unimplemented!()
    }

    fn open(path: &str) -> Self {
        unimplemented!()
    }

    fn next_key_id(&mut self) -> OpId {
        unimplemented!()
    }

    fn next_sstable_id(&mut self) -> SStableId {
        unimplemented!()
    }

    fn update_sstable(&mut self, new_sstables: Vec<SStableId>, delete_sstables: Vec<SStableId>) {
        unimplemented!()
    }
}
