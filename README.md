# README


This is basically a progress tracker.



## 30/04/24

- Created a KV store (maybe not so efficient) that persists to disk by dumping to disk as JSON at each write.
- Added concurrency by adopting a client-server model (hardcoded ports et al), so we have a server in package _distdb_ and client that can be initialized in package _distdbclient_.
- Usage: < executable > _server_ or < executable > _client_ to launch either.
- Once you have launched a server you can connect as many clients to it as you want, use keywords _GET_ and _PUT_ (case-sensitive) to control client actions