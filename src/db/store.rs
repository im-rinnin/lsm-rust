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
    fn write_at(&mut self, position: usize);
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
    fn write_at(&mut self, position: usize) {
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
    fn write_at(&mut self, position: usize) {
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

    #[test]
    fn test_read_at_memstore() {
        let id = "test_read_at".to_string();
        let mut m = Memstore::new(&id);
        let data = b"0123456789abcdef";
        m.append(data);
        let original_pos = m.store.borrow().position();
        assert_eq!(original_pos, data.len() as u64);

        // Read from offset 5, length 4
        let mut read_buf = [0u8; 4];
        m.read_at(&mut read_buf, 5);
        assert_eq!(&read_buf, b"5678");
        // Check position is restored
        assert_eq!(m.store.borrow().position(), original_pos);

        // Read from offset 0, length 10
        let mut read_buf_2 = [0u8; 10];
        m.read_at(&mut read_buf_2, 0);
        assert_eq!(&read_buf_2, b"0123456789");
        assert_eq!(m.store.borrow().position(), original_pos);
    }
    #[test]
    fn test_write_at_memstore() {
        let id = "test_write_at".to_string();
        let mut m = Memstore::new(&id);
        let initial_data = b"initial";
        m.append(initial_data);
        let initial_pos = m.store.borrow().position();
        assert_eq!(initial_pos, initial_data.len() as u64);

        // Test seeking forward
        let seek_pos = initial_pos as usize + 10;
        m.write_at(seek_pos);
        assert_eq!(m.store.borrow().position(), seek_pos as u64);

        // Test seeking to current position (should work)
        m.write_at(seek_pos);
        assert_eq!(m.store.borrow().position(), seek_pos as u64);
    }

    #[test]
    #[should_panic]
    fn test_write_at_memstore_panic() {
        let mut m = Memstore::new(&"panic_test".to_string());
        m.append(b"some data");
        m.write_at(1); // Seek backwards, should panic due to assert
    }
}
