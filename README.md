This crate exists because I need an async (either runtime-agnostic or tokio-based) rust
memcached client that uses metadata when deserializing. Current implementations that I
know of don't allow me to do that:

* rsmc: tokio 1.x, no access to metadata
* memecached: async-std, ??
* memcache: synchronous, access to metadata
* memcache-async: ??, no access to metadata
* async-memcached: tokio 0.1, ??
* memcached-rs: synchronous, ??
* vmemcached: ??, no access to metadata

It will most likely break at some point.
It's NOT production ready.
