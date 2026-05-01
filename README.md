# AkitaDB

A database engine built from scratch in Java with the sole aim of understanding database internals from the ground up.

## About

AkitaDB is a work-in-progress implementation of core database engine components. The goal is not to build a production database, but to deeply understand how databases work internally by actually building one. 
The name is inspired by the Akita, a dog known for being loyal and persistent.

## What's Been Built

### Disk Storage Layer
Handles reading and writing data to disk at the page level. This is the lowest layer of the engine, responsible for managing how data is physically laid out and accessed on disk.

### Buffer Pool Manager
Sits on top of the disk layer and manages an in-memory cache of pages. Rather than going to disk for every read or write, the buffer pool keeps frequently used pages in memory and handles eviction when space runs out.

## Roadmap

- [x] Disk storage layer
- [x] Buffer pool manager
- [ ] Heap file / page layout (in progress)
- [ ] Tuple representation and slotted pages (in progress)
- [ ] B+ Tree index
- [ ] Query execution engine
- [ ] Transaction management & concurrency control
- [ ] Recovery (WAL / ARIES)

## References

- [CMU 15-445/645 — Database Systems](https://15445.courses.cs.cmu.edu/) by Andy Pavlo
- *Database Systems: The Complete Book* — Garcia-Molina, Ullman, Widom
- Various database research papers
- AI has been extremely helpful so far in clarifications of thoughts

## Testing

Requires Java 21+ and Maven.

```bash
git clone https://github.com/AmosAidoo/akitadb.git
cd akitadb
mvn clean verify
```
