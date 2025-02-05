use std::collections::HashMap;

use super::common::Result;
pub type StoreId = String;
pub trait Store {
    fn open(&mut self, id: &StoreId);
    fn flush(&mut self, id: &StoreId);
    fn append(&mut self, id: &StoreId, data: &[u8]);
    fn read_at(&self, id: &StoreId, buf: &mut [u8], offset: usize);
}

pub struct Memstore {
    store_map: HashMap<StoreId, Vec<u8>>,
}
pub struct Filestore {}

impl Memstore {
    fn new() -> Self {
        Memstore {
            store_map: HashMap::new(),
        }
    }
    fn open(data: HashMap<StoreId, Vec<u8>>) -> Self {
        Memstore { store_map: data }
    }
}

impl Store for Memstore {
    fn flush(&mut self, id: &StoreId) {}
    fn open(&mut self, id: &StoreId) {
        self.store_map.insert(id.to_string(), Vec::new());
    }
    fn append(&mut self, id: &StoreId, data: &[u8]) {
        let v = self.store_map.get_mut(id).expect("id not found");
        v.extend_from_slice(data);
    }
    fn read_at(&self, id: &StoreId, buf: &mut [u8], offset: usize) {
        let v = self.store_map.get(id).expect("id not found");
        let res = v.as_slice();
        buf.copy_from_slice(&res[offset..offset + buf.len()]);
    }
}

impl Filestore {
    fn open(dir_path: String) -> Self {
        unimplemented!()
    }
}
impl Store for Filestore {
    fn flush(&mut self, id: &StoreId) {
        unimplemented!()
    }
    fn open(&mut self, id: &StoreId) {
        unimplemented!()
    }
    fn append(&mut self, id: &StoreId, data: &[u8]) {}
    fn read_at(&self, id: &StoreId, buf: &mut [u8], offset: usize) {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use std::io::{Read, Write};

    use serde::{Deserialize, Serialize};

    use super::{Memstore, Store};
    #[derive(Serialize, Deserialize, Debug)]
    struct TestSerde {
        pub a: i32,
        pub b: String,
        pub c: Vec<i32>,
    }

    #[test]
    fn test_read_write_memsotre() {
        let mut m = Memstore::new();
        let id = String::from("1");
        m.open(&id);
        let t = TestSerde {
            a: 1,
            b: "sdfs".to_string(),
            c: vec![1, 2, 3],
        };
        let mut a = Vec::new();
        bincode::serialize_into(&mut a, &t);
        bincode::serialize_into(&mut a, &t);
        m.append(&id, &a);
        let mut data = vec![0; a.len()];
        m.read_at(&id, data.as_mut_slice(), 0);
        let mut s = data.as_slice();
        let d: TestSerde = bincode::deserialize_from(&mut s).unwrap();
        let t: TestSerde = bincode::deserialize_from(&mut s).unwrap();
        println!("{:?}", d);
        println!("{:?}", t);
    }
}
