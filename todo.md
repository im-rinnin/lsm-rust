# LSM-Tree Key-Value Store Development Roadmap
learn lsm and rust 
and how to profile code

## in progress
open db from exit db file
- [x] when db stop, need to dump all memtable and imm table to level 0
- [ ] load table change file
- [ ] load sstable and build level storage

## hight 
read mini lsm and refactor
- [ ] test run new db  from file
##  normal
- [ ] loop fetch write request in write_worker, dont sleep
- [ ] test disk Performance for read and sequence write
- [ ] File-based storage backend implementation
- [ ] support range qurey
- [ ] test start db from file 
- [ ] compact level change in table change log
## low
- [ ] Performance benchmarking
- [ ] Recovery mechanism
## done
read mini lsm and refactor
- [x] freeze memtable in write worker
- [x] check if need comapct after compact loop end and run compact immediately if needed
- [x] add ut for db
- [x] delete kv operation in max  level table 
- [x] limit write rate if too many imm memtable not dump
- [x] Key-Value use key(vec) and keyslice, see mini lsm
- [x] refactor optype
- [x] block use mini lsm data format
- [x] iterator try to simplify it 
- [x] ai review code
- [x] remove kvopref use &kvop
- [x] zero copy kv read
- [x] set a limit to compact input table number(only one)
- [x] add config to all struct 
- [x] add log
- [x] add compact test 
- [x] nvim debug  rust
- [x] Block storage layer (completed)
- [x] Thread-safe high-performance memtable implementation




