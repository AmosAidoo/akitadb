# AkitaDB

AkitaDB is a from-scratch Java database engine for learning database internals by
building them directly: disk pages, a buffer pool, heap files, B+ trees, a small
catalog, SQL parsing/binding, planning, and tuple-at-a-time execution.

It is not trying to be production database software yet. It is a compact engine
lab with real storage and query execution pieces.

## Build

Requires Java 21+ and Maven.

```bash
./mvnw clean verify
```

The repository is a Maven multi-module project:

```text
akita-parent
|-- core/        engine code and tests
`-- benchmarks/  JMH-style microbenchmarks for hot internals
```

## Engine Map

```text
SQL text
  |
  v
Lexer -> Parser -> AST
  |
  v
Binder + Catalog
  |
  v
LogicalPlan -> PhysicalPlan
  |
  v
Executor tree -> Row stream
  |
  v
HeapFile / B+Tree
  |
  v
BufferPoolManager
  |
  v
DiskScheduler
  |
  v
BlockManager / VFS
  |
  v
container files on disk
```

Main packages:

| Package | Role |
| --- | --- |
| `com.akita.storage` | container files, block I/O, VFS adapters, page allocation |
| `com.akita.buffer` | frames, page table, guards, disk scheduler, ARC replacement |
| `com.akita.page` | page headers, slots, tuples, slotted-page layout, page directory |
| `com.akita.heap` | heap-file open/scan/insert and heap page access |
| `com.akita.index.btree` | B+ tree pages, tuple serialization, insertion, lookup, DOT dump |
| `com.akita.catalog` | table/index metadata and JSON catalog persistence |
| `com.akita.datatype` | schemas, column metadata, typed values, comparisons |
| `com.akita.sql` | lexer, parser, AST for the supported SQL subset |
| `com.akita.query` | binding, logical planning, physical planning, executors, cursors |

## On-Disk Shape

Storage uses fixed-size 8 KiB blocks. A `ContainerId` maps to one physical
container file through the VFS layer. Higher layers treat each block as a page:

```text
PageId = (ContainerId, blockNumber)

container file
|-- block 0  page directory / object header
|-- block 1  root page, heap page, or data page depending on object type
|-- block 2  heap/index page
`-- block N  heap/index page
```

The current `FileChannelBlockManager` grows files by writing zeroed 8 KiB blocks
up to the requested block number, then reads and writes blocks at
`blockNumber * 8192`.

### Slotted Pages

Most logical pages share the same base layout:

```text
8 KiB page
+---------------------+  offset 0
| PageHeader          |  numberOfSlots: short
+---------------------+
| extended header     |  page-type-specific fields
+---------------------+
| Slot[0]             |  offset: short, length: short
| Slot[1]             |
| ...                 |
+---------------------+
| free space          |
+---------------------+
| tuple bytes         |
| tuple bytes         |
+---------------------+  offset 8192
```

Slots grow forward from the header. Tuple bytes grow backward from the end of
the page. A slot points to the tuple's byte range.

### Page Directory

Block `0` is special for heap-like objects. It starts with a small object header,
then stores page-directory entries as slotted tuples:

| Field | Meaning |
| --- | --- |
| `HeapFileHeader.type` | `TABLE` or `INDEX` |
| `PageDirectory.nextBlockPointer` | next directory page, or `0` |
| directory tuple | `(blockNumber: long, freeSpace: int)` |

The directory is used to find heap pages with enough free space and to bootstrap
the next block allocator from the highest known block number.

### B+ Tree Pages

B+ tree pages extend the slotted page header:

```text
PageHeader
pageType: byte                 1 = internal, 2 = leaf
pageTypeSpecificBlock: long    rightmost child or next leaf
slot directory
key tuples
```

| Page type | Tuples contain | Extra pointer |
| --- | --- | --- |
| leaf | indexed columns plus `RecordId` | next leaf block |
| internal | separator key plus left child block | rightmost child block |

The root page is currently block `1`. Inserts split full leaves and internal
pages, promoting separator keys upward.

## Buffering And I/O

```text
BufferPoolManager
|-- pageTable: PageId -> Frame
|-- freeFrames: unused frames
|-- replacer: ARC frame eviction
`-- DiskScheduler -> BlockManager

readPage(page)
  hit:  record access, return ReadPageGuard
  miss: reserve frame, flush victim if dirty, read block, return guard

writePage(page)
  hit:  record access, return WritePageGuard
  miss: reserve frame, flush victim if dirty, read block, return guard

allocatePage(page)
  allocate disk block, reserve frame, install empty page, return WritePageGuard
```

Guards pin pages while they are in use and unpin them when closed. Dirty frames
are snapshotted and flushed before reuse.

## Query Pipeline

The first query path supports single-table `SELECT` statements with projections,
filters, literals, identifiers, arithmetic/comparison expressions, and boolean
operators.

```text
Parser
  SelectStatement
       |
       v
Binder
  BoundSelectStatement       names resolved against Catalog
       |
       v
LogicalPlanner
  Projection
    Filter
      Scan
       |
       v
PhysicalPlanner
  ProjectionPlan
    FilterPlan
      SeqScanPlan
       |
       v
Executors
  ProjectionExecutor
    FilterExecutor
      SeqScanExecutor
```

`QueryEngine.execute(sql)` materializes all rows. `QueryEngine.query(sql)`
returns a `QueryCursor` so callers can consume rows incrementally.

## Catalog

`JsonCatalog` can run in-memory or persist to a JSON file. The persisted shape is
kept intentionally small:

```text
catalog.json
`-- tables[]
    |-- tableName
    |-- containerId
    `-- columns[]
        |-- name
        |-- type
        |-- ordinalPosition
        `-- nullable
```

Supported types currently include `INTEGER`, `BIGINT`, `DOUBLE`, `BOOLEAN`, and
`VARCHAR(n)`.

## Current Scope

Implemented:

| Area | Status |
| --- | --- |
| fixed-size block storage | working |
| file-backed containers and VFS | working |
| buffer pool with guards and ARC replacement | working |
| slotted pages and page directories | working |
| heap file scan/get/insert path | partial but exercised |
| B+ tree insertion/search primitives | working in tests |
| JSON catalog | working |
| SQL lexer/parser/AST | working for the initial subset |
| binder, logical planner, physical planner | working for single-table selects |
| sequential scan, filter, projection executors | working |

Not implemented yet:

| Area | Notes |
| --- | --- |
| joins and aggregates | out of the first query milestone |
| cost-based optimization | planner currently maps logical nodes directly |
| index selection | B+ tree exists, optimizer does not choose it yet |
| transactions/concurrency control | page latching exists, transactional semantics do not |
| WAL/recovery | not started |
| SQL DDL/DML surface | metadata and heap APIs exist below SQL |

## References

- [CMU 15-445/645 Database Systems](https://15445.courses.cs.cmu.edu/)
- *Database Systems: The Complete Book* by Garcia-Molina, Ullman, and Widom
