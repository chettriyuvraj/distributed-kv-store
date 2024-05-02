# README


This is basically a progress tracker.



## 30/04/24

- Created a KV store (maybe not so efficient) that persists to disk by dumping to disk as JSON at each write.
- Added concurrency by adopting a client-server model (hardcoded ports et al), so we have a server in package _distdb_ and client that can be initialized in package _distdbclient_.
- Usage: < executable > _server_ or < executable > _client_ to launch either.
- Once you have launched a server you can connect as many clients to it as you want, use keywords _GET_ and _PUT_ (case-sensitive) to control client actions


## 03/04/24

- Extended KV store to use protobuf at both client and server
- Defined schemas for Request and Response and generated protobuf class using the protobuf code generator
- Self note: always set 0 values for enums as "DUMMY"; had a bug where 0 value enum was my response + all other fields were empty i.e. default val - so basically data was all 0's and nothing was being sent
TODOS:
- JSON still used to persist the data - change this to protobuf as well(?)
- Add benchmarks to compare old and new implementation?