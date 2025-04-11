use std::{
    cell::RefCell,
    collections::HashMap,
    io::{Cursor, Read, Write},
    usize,
};

use byteorder::WriteBytesExt;

use super::common::Result;
pub type StoreId = String;
pub trait Store {
    fn new(id: &StoreId) -> Self;
    fn flush(&mut self);
    fn append(&mut self, data: &[u8]);
    fn read_at(&self, buf: &mut [u8], offset: usize);
    // store is append only, so seek positon should >= current positon
    fn write_seek(&mut self, position: usize);
    fn len(&self) -> usize;
    fn close(self);
    fn get_id(&self) -> StoreId;
}

pub struct Memstore {
    store: RefCell<Cursor<Vec<u8>>>,
    id: StoreId,
}
pub struct Filestore {}

impl Memstore {
    fn end(self) -> Vec<u8> {
        self.store.take().into_inner()
    }
}

impl Store for Memstore {
    fn close(self) {}
    fn new(id: &StoreId) -> Self {
        Memstore {
            store: RefCell::new(Cursor::new(Vec::new())),
            id: id.clone(),
        }
    }
    fn len(&self) -> usize {
        self.store.borrow().position() as usize
    }
    fn flush(&mut self) {}
    fn append(&mut self, data: &[u8]) {
        self.store.borrow_mut().write(data);
    }
    fn read_at(&self, buf: &mut [u8], offset: usize) {
        let position = self.store.borrow().position();
        self.store.borrow_mut().set_position(offset as u64);
        let res = self.store.borrow_mut().read(buf).unwrap();
        self.store.borrow_mut().set_position(position);
    }
    fn write_seek(&mut self, position: usize) {
        assert!(self.store.borrow().position() as usize <= position);
        self.store.borrow_mut().set_position(position as u64);
    }

    fn get_id(&self) -> StoreId {
        self.id.clone()
    }
}

impl Filestore {
    fn open(dir_path: String) -> Self {
        unimplemented!()
    }
}
impl Store for Filestore {
    fn flush(&mut self) {
        unimplemented!()
    }
    fn new(id: &StoreId) -> Self {
        todo!()
    }
    fn len(&self) -> usize {
        todo!()
    }
    fn close(self) {
        todo!()
    }
    fn append(&mut self, data: &[u8]) {}
    fn read_at(&self, buf: &mut [u8], offset: usize) {
        unimplemented!()
    }
    fn write_seek(&mut self, position: usize) {
        todo!()
    }
    fn get_id(&self) -> StoreId {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::{
        io::{Read, Write},
        str::FromStr,
    };

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
        let id = "1".to_string();
        let mut m = Memstore::new(&id);
        let id = String::from("1");
        let t = TestSerde {
            a: 1,
            b: "sdfs".to_string(),
            c: vec![1, 2, 3],
        };
        let mut a = Vec::new();
        bincode::serialize_into(&mut a, &t);
        bincode::serialize_into(&mut a, &t);
        m.append(&a);
        let mut data = vec![0; a.len()];
        m.read_at(data.as_mut_slice(), 0);
        let mut s = data.as_slice();
        let d: TestSerde = bincode::deserialize_from(&mut s).unwrap();
        let t: TestSerde = bincode::deserialize_from(&mut s).unwrap();
    }
}
