This crate exists because I need an async (either runtime-agnostic or tokio-based) rust
memcached client that uses metadata when deserializing. Current implementations that I
know of don't allow me to do that:

* rsmc: tokio 1.x, no access to metadata
* memcached: async-std, no access to metadata
* memcache: synchronous, access to metadata
* memcache-async: futures, no access to metadata
* async-memcached: tokio 0.2, access to metadata
* memcached-rs: synchronous, access to metadata
* vmemcached: tokio 1.x, no access to metadata

It will most likely break at some point.
It's NOT production ready.
