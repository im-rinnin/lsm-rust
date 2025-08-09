# db test 


mutiple thread write get

3 thread do write delete get in random order

create db enable auto compact
create a thread safe hashmap call data_states, contains all data latest state map<arc<mutex(key,optype)>>
fill db with init data key from [0..1000],set them to hashmap data_states
start 3 thread  random pick key in range 0..3000, pick random operaiotn (write/put/get),do it, compare result with data_states(if read),update data_states 

