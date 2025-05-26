use crate::db::key::KeySlice;
use crate::db::key::ValueSlice;
use std::{
    cmp::Ordering,
    io::{Cursor, Read, Write},
    rc::Rc,
    thread::sleep,
    usize,
};

use byteorder::WriteBytesExt;
use byteorder::{LittleEndian, ReadBytesExt};
use std::mem::size_of;

use super::key::{KeyVec, ValueVec}; // Added for kv_opertion_len // Added for kv_opertion_len

pub type Value<'a> = &'a [u8];

pub struct KeyQuery {
    pub key: KeyVec,
    pub op_id: OpId,
}

pub type Buffer = Cursor<Vec<u8>>;
pub fn new_buffer(size: usize) -> Buffer {
    Cursor::new(vec![0; size])
}
pub enum Error {}
#[derive(Debug, PartialEq, Eq)]
pub struct KVOpertionRef<'a> {
    pub id: OpId,
    pub key: KeySlice<'a>,
    pub op: OpTypeRef<'a>,
}
impl<'a> KVOpertionRef<'a> {
    pub fn from_op(op: &'a KVOpertion) -> Self {
        let op_type_ref = match &op.op {
            OpType::Write(v) => OpTypeRef::Write(v.as_ref().into()),
            OpType::Delete => OpTypeRef::Delete,
        };
        KVOpertionRef {
            id: op.id,
            key: op.key.as_ref().into(),
            op: op_type_ref,
        }
    }
    pub fn encode(&self, w: &mut Buffer) {
        w.write_u64::<LittleEndian>(self.id).unwrap();
        let key_len = self.key.len() as u64;
        w.write_u64::<LittleEndian>(key_len).unwrap();
        w.write_all(self.key.as_ref()).unwrap();
        match &self.op {
            OpTypeRef::Delete => {
                w.write_u8(0).unwrap();
            }
            OpTypeRef::Write(v) => {
                w.write_u8(1).unwrap();
                let v_len = v.len() as u64;
                w.write_u64::<LittleEndian>(v_len).unwrap();
                w.write_all(v.as_ref()).unwrap();
            }
        }
    }
    pub fn decode(r: &'a [u8]) -> Self {
        let mut cursor = Cursor::new(r);
        let id = cursor.read_u64::<LittleEndian>().unwrap();
        let key_len = cursor.read_u64::<LittleEndian>().unwrap() as usize;
        let key_start = cursor.position() as usize;
        let key_end = key_start + key_len;
        let key = KeySlice::from(&r[key_start..key_end]);

        cursor.set_position(key_end as u64);
        let op_type = cursor.read_u8().unwrap();
        let op = match op_type {
            0 => OpTypeRef::Delete,
            _ => {
                let value_len = cursor.read_u64::<LittleEndian>().unwrap() as usize;
                let value_start = cursor.position() as usize;
                let value_end = value_start + value_len;
                let value = ValueSlice::from(&r[value_start..value_end]);
                OpTypeRef::Write(value)
            }
        };
        KVOpertionRef { id, key, op }
    }
    pub fn encode_size(&self) -> usize {
        // id (u64) + key_len (u64) + key_data
        let mut size = size_of::<u64>() + size_of::<u64>() + self.key.len();
        // op_type (u8)
        size += size_of::<u8>();
        if let OpTypeRef::Write(v) = &self.op {
            // value_len (u64) + value_data
            size += size_of::<u64>() + v.len();
        }
        size
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct KVOpertion {
    pub id: OpId,
    pub key: KeyVec,
    pub op: OpType,
}
impl KVOpertion {
    pub fn new(id: OpId, key: KeyVec, op: OpType) -> Self {
        KVOpertion {
            id: id,
            key: key,
            op: op,
        }
    }
    pub fn from_ref(op_ref: KVOpertionRef) -> Self {
        let op_type = match op_ref.op {
            OpTypeRef::Write(v) => OpType::Write(ValueVec::from_vec(v.as_ref().to_vec())),
            OpTypeRef::Delete => OpType::Delete,
        };
        KVOpertion {
            id: op_ref.id,
            key: KeyVec::from_vec(op_ref.key.as_ref().to_vec()),
            op: op_type,
        }
    }

    pub fn encode_size(&self) -> usize {
        KVOpertionRef::from_op(self).encode_size()
    }
    pub fn decode(r: &mut Buffer) -> Self {
        let current_pos = r.position() as usize;
        let remaining_data = &r.get_ref()[current_pos..];
        let op_ref = KVOpertionRef::decode(remaining_data);
        let size = op_ref.encode_size();
        let res = KVOpertion::from_ref(op_ref);
        r.set_position((current_pos + size) as u64);
        res
    }
    pub fn encode(&self, w: &mut Buffer) {
        KVOpertionRef::from_op(self).encode(w);
    }
}
pub type Result<T> = std::result::Result<T, Error>;
// every Key in db has a unique id
pub type OpId = u64;
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)] // Added Clone
pub enum OpType {
    Write(ValueVec),
    Delete,
}
impl OpType {
    pub fn from_ref(op_ref: OpTypeRef) -> Self {
        match op_ref {
            OpTypeRef::Write(v) => OpType::Write(ValueVec::from_vec(v.as_ref().to_vec())),
            OpTypeRef::Delete => OpType::Delete,
        }
    }
}
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum OpTypeRef<'a> {
    Write(ValueSlice<'a>),
    Delete,
}

pub struct KViterAgg<'a> {
    iters: Vec<&'a mut dyn Iterator<Item = &'a KVOpertion>>,
    iters_next: Vec<Option<&'a KVOpertion>>,
}
impl<'a> KViterAgg<'a> {
    pub fn new(iters: Vec<&'a mut dyn Iterator<Item = &'a KVOpertion>>) -> Self {
        let mut res = KViterAgg {
            iters,
            iters_next: Vec::new(),
        };
        for it in res.iters.iter_mut() {
            res.iters_next.push(it.next());
        }
        res
    }
}

impl<'a> Iterator for KViterAgg<'a> {
    type Item = &'a KVOpertion;
    fn next(&mut self) -> Option<Self::Item> {
        let mut has_next = false;
        for (i, n) in self.iters_next.iter_mut().enumerate() {
            if n.is_none() {
                *n = self.iters.get_mut(i).unwrap().next();
            }
            if n.is_some() {
                has_next = true;
            }
        }
        // at least one iter in iters_next so we can just unwrap
        if has_next == false {
            return None;
        }

        let mut kv_smallest_key: Option<&KVOpertion> = None;
        let mut position = Vec::new();
        for (index, current_kv) in self.iters_next.iter_mut().enumerate() {
            if let Some(v) = current_kv {
                match kv_smallest_key {
                    None => {
                        kv_smallest_key = current_kv.clone();
                        assert_eq!(position.len(), 0);
                        position.push(index);
                    }
                    Some(ref kv_res) => {
                        let cmp_res = kv_res.key.cmp(&v.key);
                        match cmp_res {
                            Ordering::Less => { //nothing
                            }
                            Ordering::Equal => {
                                let id_cmp = kv_res.id.cmp(&v.id);
                                match id_cmp {
                                    Ordering::Greater => {
                                        position.push(index);
                                    }
                                    Ordering::Equal => {
                                        panic!("should not be same id ")
                                    }
                                    Ordering::Less => {
                                        kv_smallest_key = current_kv.clone();
                                        position.push(index);
                                    }
                                }
                                assert!(position.len() > 1);
                            }
                            Ordering::Greater => {
                                kv_smallest_key = current_kv.clone();
                                position.clear();
                                position.push(index);
                            }
                        }
                    }
                }
            }
        }
        assert!(kv_smallest_key.is_some());
        // remove all other kv same key but id is less
        for position in &position {
            let t = self.iters_next.get_mut(*position).unwrap();
            *t = None;
        }
        kv_smallest_key
    }
}
pub mod test {
    use super::KVOpertionRef;
    use super::OpTypeRef;
    use crate::db::common::{new_buffer, KVOpertion, KViterAgg};

    use super::OpType;

    #[test]
    fn test_kv_iter_agg() {
        // input iter :diff length ,diff order ,no duplicte key in same iter but in diff
        // iter,ordered in same iter
        // [(id,key)]
        let a = vec![(1, 1), (3, 3), (4, 4), (7, 7), (10, 99)];
        let b = vec![(2, 2), (5, 3), (6, 6), (28, 8), (9, 9)];
        let c = vec![(11, 3), (18, 8)];

        let f = |a: &(u64, u64)| -> KVOpertion {
            let id = a.0;
            let key = a.1.to_string();
            KVOpertion::new(
                id,
                key.as_bytes().into(),
                OpType::Write(key.to_string().as_bytes().into()),
            )
        };

        let a_ops: Vec<KVOpertion> = a.iter().map(f).collect();
        let mut a_iter_ref = a_ops.iter();
        let b_ops: Vec<_> = b.iter().map(f).collect();
        let mut b_iter_ref = b_ops.iter();
        let c_ops: Vec<_> = c.iter().map(f).collect();
        let mut c_iter_ref = c_ops.iter();

        let kv_iter = KViterAgg::new(vec![&mut a_iter_ref, &mut b_iter_ref, &mut c_iter_ref]);
        let expect = vec![
            (1, 1),
            (2, 2),
            (11, 3),
            (4, 4),
            (6, 6),
            (7, 7),
            (28, 8),
            (9, 9),
            (10, 99),
        ];
        let output_ids: Vec<(u64, u64)> = kv_iter
            // Convert KeyVec to String before parsing
            .map(|i| {
                (
                    i.id,
                    String::from_utf8_lossy(i.key.inner())
                        .parse::<u64>()
                        .unwrap(),
                )
            })
            .collect();
        assert_eq!(output_ids, expect);
    }

    #[test]
    fn test_kv_operation_size() {
        let op = KVOpertion {
            id: 1,
            key: "123".as_bytes().into(),
            op: OpType::Delete,
        };

        assert_eq!(20, op.encode_size());

        let op = KVOpertion {
            id: 1,
            key: "123".as_bytes().into(),
            op: OpType::Write("234".as_bytes().into()),
        };
        assert_eq!(31, op.encode_size());
    }
    #[test]
    fn test_kv_operation_encode() {
        let op = KVOpertion {
            id: 1,
            key: "123".to_string().as_bytes().into(),
            op: OpType::Delete,
        };
        let mut v = new_buffer(1024);
        op.encode(&mut v);
        assert_eq!(v.position() as usize, op.encode_size());
        v.set_position(0);
        let op_res = KVOpertion::decode(&mut v);
        assert_eq!(op_res, op);

        let op = KVOpertion {
            id: 1,
            key: "123".to_string().as_bytes().into(),
            op: OpType::Write("234".as_bytes().into()),
        };
        let mut v = new_buffer(1024);
        op.encode(&mut v);
        assert_eq!(v.position() as usize, op.encode_size());
        v.set_position(0);
        let op_res = KVOpertion::decode(&mut v);
        assert_eq!(op_res, op);
    }

    #[test]
    fn test_kv_opertion_ref_from_op() {
        // Test with Write operation
        let write_op = KVOpertion {
            id: 10,
            key: "test_key".as_bytes().into(),
            op: OpType::Write("test_value".as_bytes().into()),
        };
        let write_op_ref = KVOpertionRef::from_op(&write_op);

        assert_eq!(write_op_ref.id, 10);
        assert_eq!(write_op_ref.key.as_ref(), b"test_key");
        match write_op_ref.op {
            OpTypeRef::Write(v) => assert_eq!(v.as_ref(), b"test_value"),
            OpTypeRef::Delete => panic!("Expected Write operation"),
        }

        // Test with Delete operation
        let delete_op = KVOpertion {
            id: 20,
            key: "another_key".as_bytes().into(),
            op: OpType::Delete,
        };
        let delete_op_ref = KVOpertionRef::from_op(&delete_op);

        assert_eq!(delete_op_ref.id, 20);
        assert_eq!(delete_op_ref.key.as_ref(), b"another_key");
        match delete_op_ref.op {
            OpTypeRef::Write(_) => panic!("Expected Delete operation"),
            OpTypeRef::Delete => {} // Correct
        }
    }
}
