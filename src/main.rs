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

mod db;
fn main() {
    let a: std::ops::Range<i32> = 1..2;
    let mut c = Cursor::new(a);
    c.set_position(1);
}
