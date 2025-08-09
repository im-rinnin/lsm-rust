# Design

lsm-rust is an LSM-based key-value database.
LSM is an append-only key-value store, where all data changes are stored. Compaction optimizes the data.
Append-only design allows us to implement snapshots easily.

## data design


## Core Components

### DB
The user operation entry point and the entry point for all core processes of the DB.
Includes the following structures:

### Memtable
Thread-safe, high-performance key-value map.

### Logfile
Append-only log to save key-value operations, aiming for performance improvement (writes) and failure recovery.

### level Meta
Metadata information for all tables (filenames) and level metadata.
Metadata changes require a file flush to ensure safety.
need a lock to provide multithread read/update

### Level
Composed of multiple tables, used for querying and compaction.

#### Table
An ordered array of key-value pairs, ordered by key, corresponding to a file. 
Level 0 tables are dumped from memtables, so they may contain duplicate keys and are sorted by key ID if keys are duplicated.
Level n (n>1) does not contain duplicate keys.

##### Block

## Core Processes


### Write/Delete
support mutile write thread
two part
client thread, submit write query to channel
write thread, do the actual write
all write query to a channel
a thread complete write operation, get write query from channel, wait channel reaching a len or timeout, do write
### client thread 
fetch and update max op submit write query,wait for condition variables
#### worker threadworker threadworker threadworker threadworker thread
* write to log file  (lock and release)
* Write to memtable.
* notify client by condition variable

### compact thread 
only one compact thread
* check if the memtable size reaches the limit, change it to immutable and create a new memtable for writes. (lock memtable of db and release)
* dump the memtable as an SSTable, save it to level one, do compact level by level, save the table changes to the change log file.
* lock and update db current lsm storage 


### Read/Snapshot Read
supoort mutiple thread
get current lsm  (need read lock and release)
Read from memtable.
Read from immutable memtable.
Read from level 0 to level n.


### sstable gc
how to delete unuesd sstable file
#### when db running
gc thread 
contains all table since db start
check every n minutes
find all table which arc count is one (only this thread contain )
delete the file 
#### when db start
delete all file not in lsm storage



## Optimization
cache for meta 

### Batch Write

## Profile
TODO
