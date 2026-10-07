# Collections

Generic helpers and concurrency-safe data structures that the standard library
does not provide. Small, dependency-free utilities shared across the router.

| Symbol | Role |
|--------|------|
| `SliceMap` | Apply a function to each element of a slice, returning a new slice. |
| `Bitset` | Value-type bitset with inline 256-bit storage and on-demand overflow words. |
| `AtomicBitset` | Lock-free concurrent bitset with atomic set, clear, and snapshot operations. |
| `Interner` | Dense non-negative integer ordinal interner with free-list recycling and lock-free reverse lookup. |
| `FastTable` | Bounded 8-probe lock-free pointer cache with authoritative negative lookups prior to overflow. |
| `ShardedBitsetIndex` | 256-shard `RWMutex` map and `FastTable` indexing `BitsetEntry` values with lazy dead-entry compaction. |
| `BitsetEntry` | Inverted-index entry pairing a key with an `AtomicBitset` and an atomic eviction flag. |
