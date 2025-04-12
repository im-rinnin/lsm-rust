use std::{
    cmp::Ordering,
    io::{Cursor, Read, Write},
    rc::Rc,
    usize,
};

use byteorder::WriteBytesExt;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use std::mem::size_of; // Added for kv_opertion_len

pub type Key = String;
pub type Value = String;

const KEY_SIZE_LIMIT: usize = 128;
const VALUE_SIZE_LIMIT: usize = 300;

pub struct KeyQuery {
    pub key: Key,
    pub op_id: OpId,
}

pub type Buffer = Cursor<Vec<u8>>;
// todo! add buffer pool
pub fn new_buffer(size: usize) -> Buffer {
    Cursor::new(vec![0; size])
}
pub enum Error {}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct KVOpertionRef<'a> {
    pub id: &'a OpId,
    pub key: &'a String,
    pub op: &'a OpType,
}

impl<'a> KVOpertionRef<'a> {
    pub fn new(kv_op: &'a KVOpertion) -> Self {
        KVOpertionRef {
            id: &kv_op.id,
            key: &kv_op.key,
            op: &kv_op.op,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct KVOpertion {
    pub id: OpId,
    pub key: String,
    pub op: OpType,
}
impl KVOpertion {
    pub fn new(id: OpId, key: String, op: OpType) -> Self {
        KVOpertion {
            id: id,
            key: key,
            op: op,
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;
// every Key in db has a unique id
pub type OpId = u64;
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)] // Added Clone
pub enum OpType {
    Write(String),
    Delete,
}
pub fn kv_opertion_len(kv_ref: &KVOpertionRef) -> usize {
    size_of::<u64>()
        + kv_ref.key.len()
        + size_of::<u64>()
        + size_of::<u8>()
        + if let OpType::Write(v) = kv_ref.op {
            v.len() + size_of::<u64>()
        } else {
            0
        }
}
pub fn write_kv_operion(kv_opertion: &KVOpertionRef, w: &mut dyn Write) {
    w.write_u64::<LittleEndian>(*kv_opertion.id);
    let key_len = kv_opertion.key.len() as u64;
    w.write_u64::<LittleEndian>(key_len);
    w.write(kv_opertion.key.as_bytes());
    match kv_opertion.op {
        OpType::Delete => {
            w.write_u8(0);
        }
        OpType::Write(v) => {
            w.write_u8(1);
            let v_len = v.len() as u64;
            w.write_u64::<LittleEndian>(v_len);
            w.write(v.as_bytes());
        }
    }
}
pub fn read_kv_operion(r: &mut Cursor<Vec<u8>>) -> KVOpertion {
    let id = r.read_u64::<LittleEndian>().unwrap();
    let key_len = r.read_u64::<LittleEndian>().unwrap();
    let mut tmp = vec![0; key_len as usize];
    r.read_exact(&mut tmp);
    let key = String::from_utf8(tmp).unwrap();
    let op_type = r.read_u8().unwrap();
    let op = match op_type {
        0 => OpType::Delete,
        _ => {
            let value_len = r.read_u64::<LittleEndian>().unwrap();
            let mut tmp = vec![0; value_len as usize];
            r.read_exact(&mut tmp);
            let value = String::from_utf8(tmp).unwrap();
            OpType::Write(value)
        }
    };
    KVOpertion { id, key, op }
}
pub struct KViterAgg<'a> {
    iters: Vec<&'a mut dyn Iterator<Item = KVOpertionRef<'a>>>,
    iters_next: Vec<Option<KVOpertionRef<'a>>>,
}
impl<'a> KViterAgg<'a> {
    pub fn new(iters: Vec<&'a mut dyn Iterator<Item = KVOpertionRef<'a>>>) -> Self {
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
    type Item = KVOpertionRef<'a>;
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

        let mut kv_smallest_key: Option<KVOpertionRef> = None;
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
                        let cmp_res = kv_res.key.cmp(v.key);
                        match cmp_res {
                            Ordering::Less => { //nothing
                            }
                            Ordering::Equal => {
                                let id_cmp = kv_res.id.cmp(v.id);
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
    use crate::db::common::{kv_opertion_len, KVOpertion, KVOpertionRef, KViterAgg};

    use super::OpType;
    pub fn create_kv_data_for_test(size: usize) -> Vec<KVOpertion> {
        let mut v = Vec::new();

        for i in 0..size {
            let tmp = KVOpertion::new(i as u64, i.to_string(), OpType::Write(i.to_string()));
            v.push(tmp);
        }

        let it = v.iter();
        let mut out_it = it.map(|a| (&a.id, &a.key, &a.op));

        v
    }
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
            let key = a.1;
            KVOpertion::new(id, key.to_string(), OpType::Write(key.to_string()))
        };

        let a_ops: Vec<KVOpertion> = a.iter().map(f).collect();
        let mut a_iter_ref = a_ops.iter().map(|i| KVOpertionRef::new(&i));
        let b_ops: Vec<_> = b.iter().map(f).collect();
        let mut b_iter_ref = b_ops.iter().map(|i| KVOpertionRef::new(&i));
        let c_ops: Vec<_> = c.iter().map(f).collect();
        let mut c_iter_ref = c_ops.iter().map(|i| KVOpertionRef::new(&i));

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
            .map(|i| (*i.id, str::parse(i.key).unwrap()))
            .collect();
        assert_eq!(output_ids, expect);
    }

    #[test]
    fn test_kv_operation_size() {
        let op = KVOpertion {
            id: 1,
            key: "123".to_string(),
            op: OpType::Delete,
        };

        let op_ref = crate::db::common::KVOpertionRef::new(&op);
        assert_eq!(20, kv_opertion_len(&op_ref));

        let op = KVOpertion {
            id: 1,
            key: "123".to_string(),
            op: OpType::Write("234".to_string()),
        };
        let op_ref = crate::db::common::KVOpertionRef::new(&op);
        assert_eq!(31, kv_opertion_len(&op_ref));
    }
}
