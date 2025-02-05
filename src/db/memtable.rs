use std::collections::btree_map::Iter;
use std::collections::linked_list::Iter as ListIter;
use std::collections::{BTreeMap, LinkedList};
use std::fmt::Result;

use super::{KVOpertion, KeyQuery, OpId, OpType};
struct MemtableItem {
    op_id: OpId,
    op: OpType,
}
pub struct Memtable {
    table: BTreeMap<String, LinkedList<MemtableItem>>,
    max_op_id: Option<OpId>,
}

struct MemtableIterator<'a> {
    table_iter: Iter<'a, String, LinkedList<MemtableItem>>,
    list_iter: Option<ListIter<'a, MemtableItem>>,
    current_key: Option<&'a str>,
}

impl MemtableItem {
    pub fn new(op: KVOpertion) -> Self {
        MemtableItem {
            op_id: op.id,
            op: op.op,
        }
    }
}

impl Memtable {
    fn new() -> Self {
        Memtable {
            table: BTreeMap::new(),
            max_op_id: None,
        }
    }
    pub fn get(&self, q: KeyQuery) -> Option<(OpId, &str)> {
        // find key match MemtableItem retrun none if not found
        let table_found = self.table.get(&q.key);
        let found = match table_found {
            None => None,
            Some(list) => Self::find_item_in_item_list(list, q.op_id),
        };
        if found.is_none() {
            return None;
        }
        let res = found.unwrap();

        match &res.op {
            OpType::Delete => None,
            OpType::Write(v) => Some((res.op_id, &v)),
        }
    }
    // find first item whose id <= opid
    fn find_item_in_item_list(
        list: &LinkedList<MemtableItem>,
        op_id: OpId,
    ) -> Option<&MemtableItem> {
        let mut i = list.iter();
        let mut last = i.next().expect("should have at least one item");
        if last.op_id > op_id {
            return None;
        } else {
            for item in i {
                if item.op_id > op_id {
                    break;
                }
                last = item
            }
            return Some(last);
        }
    }

    pub fn insert(&mut self, op: KVOpertion) -> Result {
        // check op id is monotonically increasing
        if self.max_op_id.is_some() {
            let id = self.max_op_id.unwrap();
            if id >= op.id {
                panic!(
                    "insert a kv which op id is {} less or eq max op {} id in memtable",
                    op.id, id
                )
            }
        }

        let op_id = op.id;
        let item_found_op = self.table.get_mut(&op.key);
        let key = op.key.clone();
        let item = MemtableItem::new(op);

        match item_found_op {
            None => {
                let list = LinkedList::from([item]);
                self.table.insert(key, list);
            }
            Some(list) => {
                list.push_back(item);
            }
        }
        // update max id if insert success
        self.max_op_id = Some(op_id);
        return Ok(());
    }

    fn len(&self) -> usize {
        self.table.len()
    }

    fn to_iter<'a>(&'a self) -> MemtableIterator<'a> {
        let i = self.table.iter();
        MemtableIterator {
            table_iter: i,
            list_iter: None,
            current_key: None,
        }
    }
}

impl<'a> Iterator for MemtableIterator<'a> {
    type Item = (&'a str, &'a MemtableItem);
    fn next(&mut self) -> Option<Self::Item> {
        // get from list iter if list is some
        if let Some(iter) = self.list_iter.as_mut() {
            if let Some(item) = iter.next() {
                return Some((self.current_key.expect("should exits key"), item));
            }
        }
        // get new list from table
        if let Some((key, list)) = self.table_iter.next() {
            self.current_key = Some(key);
            let mut iter = list.iter();
            let res = Some((
                key.as_str(),
                iter.next().expect("should have at least one item"),
            ));
            self.list_iter = Some(iter);
            return res;
        } else {
            return None;
        }
    }
}

#[cfg(test)]
mod test {

    use std::{env::vars, net::SocketAddr, os::unix::process};

    use crate::db::{KVOpertion, KeyQuery, OpId, OpType};

    fn get_next_id(id: &mut OpId) -> OpId {
        let old = *id;
        *id += 1;
        old
    }

    use super::Memtable;
    // test count

    #[test]
    fn test_empty_count() {
        let mut m = Memtable::new();
        assert_eq!(m.len(), 0);
        let op = KVOpertion::new(1, 1.to_string(), OpType::Write(1.to_string()));
        m.insert(op);
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn test_insert_delete_and_get() {
        let mut m = Memtable::new();
        let mut id = 0;
        // put 1..20
        for i in 0..20 {
            let op_id = get_next_id(&mut id);
            let op = KVOpertion::new(op_id, i.to_string(), OpType::Write(i.to_string()));
            m.insert(op);
        }
        // delete 10
        let op_id = get_next_id(&mut id);
        let op = KVOpertion::new(op_id, 10.to_string(), OpType::Delete);
        m.insert(op);
        // overwirte  12 to 100
        let op_id = get_next_id(&mut id);
        let op = KVOpertion::new(op_id, 12.to_string(), OpType::Write(100.to_string()));
        m.insert(op);
        // check op id and key match in 0..10
        for i in 0..10 {
            let res = m.get(KeyQuery {
                op_id: id,
                key: i.to_string(),
            });
            assert!(res.is_some());
            assert_eq!(res.unwrap().1, i.to_string());
        }
        //check delete
        let res = m.get(KeyQuery {
            op_id: id,
            key: 10.to_string(),
        });
        assert!(res.is_none());
        // 10 is delete but still can be get by op id 10
        let res = m.get(KeyQuery {
            op_id: 10,
            key: 10.to_string(),
        });
        assert!(res.is_some());
        assert_eq!(res.unwrap().1, 10.to_string());
        // check key not exit
        let res = m.get(KeyQuery {
            op_id: id,
            key: 100.to_string(),
        });
        assert!(res.is_none());
        // check op id not match
        // key 5 op id is 5 so should not found if use op id 1
        let res = m.get(KeyQuery {
            op_id: 1,
            key: 5.to_string(),
        });
        assert!(res.is_none());
    }

    // test iterator
    #[test]
    fn test_iterator() {
        let mut m = Memtable::new();
        let mut id = 0;
        // put 0..5 out of order
        let id_unorderd = [3, 2, 1, 4, 0];
        for i in id_unorderd {
            let op = KVOpertion::new(
                get_next_id(&mut id),
                i.to_string(),
                OpType::Write(i.to_string()),
            );
            m.insert(op);
        }
        // delete 2
        let op = KVOpertion::new(get_next_id(&mut id), 2.to_string(), OpType::Delete);
        m.insert(op);
        // overwirte 3 to 100
        let op = KVOpertion::new(
            get_next_id(&mut id),
            3.to_string(),
            OpType::Write(100.to_string()),
        );
        m.insert(op);
        // do iter and check
        let mut iter = m.to_iter();
        let mut keys = Vec::new();
        let mut ops = Vec::new();
        let mut values = Vec::new();
        let mut ids = Vec::new();
        for i in iter {
            keys.push(i.0);
            ids.push(i.1.op_id);
            let op = &i.1.op;
            let v = match op {
                OpType::Write(v) => Some(str::parse::<i32>(v).unwrap()),
                OpType::Delete => None,
            };
            values.push(v);
            ops.push(&i.1.op);
        }
        assert_eq!(keys, ["0", "1", "2", "2", "3", "3", "4"]);
        assert_eq!(ids, [4, 2, 1, 5, 0, 6, 3]);
        assert_eq!(
            values,
            [Some(0), Some(1), Some(2), None, Some(3), Some(100), Some(4)]
        );
    }
}
