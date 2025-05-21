#![allow(unused)]
use std::{
    collections::HashMap,
    fs::read,
    io::{Cursor, Read, Seek, SeekFrom, Write},
    ops::Not,
    rc::Rc,
    usize,
};

use bincode::config::NativeEndian;
use byteorder::{BigEndian, LittleEndian, WriteBytesExt};
use serde::Deserialize;
struct A {
    i: i32,
    s: String,
}

fn binary_search(i: Vec<i32>, target: i32) -> Option<usize> {
    let mut start = 0;
    let mut end = i.len();
    while start != end {
        let mid = (start + end) / 2;
        let v = i.get(mid).unwrap();
        if *v == target {
            return Some(mid);
        } else if *v > target {
            end = mid;
        } else {
            start = mid;
        }
    }
    None
}

fn goo() -> i32 {
    1
}
fn test_cov(a: i32) -> i32 {
    if a > 1 {
        goo()
    } else {
        return 2;
    }
}

mod db;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn do_test_cov() {
        // test_cov(0);
    }
    #[test]
    fn test_binary_search() {
        let data = vec![1, 3, 5, 7, 9, 11];

        // Test cases where the target is found
        assert_eq!(binary_search(data.clone(), 1), Some(0));
        // Test with single element vector
    }
}

fn main() {
    let a: std::ops::Range<i32> = 1..2;
    let mut c = Cursor::new(a);
    c.set_position(1);
    test_cov(0);
    test_cov(3);
}
