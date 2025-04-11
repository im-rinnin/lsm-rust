use crate::db::store::StoreId;

use super::common::KVOpertionRef;
// all sstable create compaction read
use super::common::Key;
use super::store::Store;
use super::KVOpertion;
use super::OpId;
use super::OpType;
pub type SStableId = i64;
pub struct SStable<T: Store> {
    id: SStableId,
    store: T,
}
mod table;
mod block;

struct SStableMeta {}
struct SStableBlock {
    data: Vec<KVOpertion>,
}
pub struct SStbleIter<'a, T: Store> {
    sstable: &'a SStable<T>,
}

// type KVIter=Iterator<'a>{
//     type=(&'a Key,&a Value)
// }
impl<T: Store> SStable<T> {
    // read by key
    pub fn read(&self, key: &Key, id: OpId) {
        unimplemented!()
    }
    // new with store and store id
    pub fn from_store(id: StoreId) -> Self {
        unimplemented!()
    }
    // build from kv iterator
    pub fn from_iter(iters: Vec<&mut dyn Iterator<Item = KVOpertionRef>>) -> Self {
        unimplemented!()
    }
    // to iter
    //
}

fn id_to_store_name(id: SStableId) -> String {
    unimplemented!()
}

impl<'a, T: Store> Iterator for SStbleIter<'a, T> {
    type Item = KVOpertionRef<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use crate::db::common::{KVOpertion, KVOpertionRef};


    fn build_iter() -> (Vec<i32>, Vec<i32>, Vec<bool>) {
        let mut keys: Vec<i32> = (0..10).collect();
        keys.push(5);
        keys.push(6);
        let mut tmp: Vec<i32> = ((10..20).collect());
        keys.append(&mut tmp);

        let mut values = keys.clone();
        values[11] = 100;

        let mut is_write = [true; 20];
        is_write[10] = false;
        (keys, values, is_write.to_vec())
    }
    // test read and from iter
    // build  kv 0..10,delete 5,overwrite 6 to 100,insert 10..20, save them to as iter, create sstable
    // from iter, check by read
    // check store is flushed after last append

    //test to iter
    // create sstable from store, use data in above test, check iter

    fn debug(i: &mut dyn Iterator<Item = i32>) {}
}
