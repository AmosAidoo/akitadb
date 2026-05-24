# Query Engine Implementation Plan

Temporary planning tracker for the first top-down query execution milestone.
This file is intended to be deleted once the work has been transferred into GitHub issues or completed.

## Milestone 1: Single-Table SELECT Pipeline

Goal: support an end-to-end query path for a narrow but useful SQL subset:

```sql
SELECT id, name FROM users WHERE age > 18;
```

The first complete pipeline should look like:

```text
SQL string
  -> parser AST
  -> binder / semantic model
  -> logical plan
  -> physical plan
  -> executor
  -> rows
```

Out of scope for this milestone:

- [ ] Joins
- [ ] Aggregates
- [ ] Nested queries
- [ ] Correlated subqueries
- [ ] Cost-based optimization
- [ ] Index selection
- [ ] Transactions or concurrency behavior

---

## Issue 1: Add SQL Lexer

### Goal

Create a small lexer for the initial SQL subset. The lexer should convert SQL text into tokens that the parser can consume.

### Scope

- [x] Add `com.akita.sql.parser` package
- [x] Add `Token`
- [x] Add `TokenType`
- [x] Add `Lexer`
- [x] Add `ParseException` or lexer-specific exception
- [x] Support SQL keywords:
  - [x] `SELECT`
  - [x] `FROM`
  - [x] `WHERE`
  - [x] `AND`
  - [x] `OR`
  - [x] `TRUE`
  - [x] `FALSE`
  - [x] `NULL` if needed for early expression modeling
- [x] Support identifiers
- [x] Support integer literals
- [x] Support string literals
- [x] Support punctuation:
  - [x] comma
  - [x] dot
  - [x] left parenthesis
  - [x] right parenthesis
  - [x] semicolon
- [x] Support operators:
  - [x] `=`
  - [x] `!=` or `<>`
  - [x] `<`
  - [x] `<=`
  - [x] `>`
  - [x] `>=`
  - [x] `+`
  - [x] `-`
  - [x] `*`
  - [x] `/`
- [x] Ignore whitespace
- [x] Produce clear errors for unknown characters and unterminated strings

### Acceptance Criteria

- [x] Lexer tokenizes a basic `SELECT ... FROM ... WHERE ...` query
- [x] Lexer preserves enough source position information for useful error messages
- [x] Unit tests cover keywords, identifiers, literals, operators, punctuation, and invalid input

### Suggested Labels

- `query-engine`
- `parser`
- `frontend`

---

## Issue 2: Add SQL AST Model

### Goal

Introduce a syntactic AST for the first SQL subset. The AST should not know anything about catalog objects, schemas, types, or storage.

### Scope

- [x] Add `com.akita.sql.ast` package
- [x] Add statement model:
  - [x] `Statement`
  - [x] `SelectStatement`
- [x] Add select-list model:
  - [x] `SelectItem`
  - [x] optional alias field if useful
- [x] Add table reference model:
  - [x] `TableRef`
  - [x] optional alias field if useful
- [x] Add expression model:
  - [x] `Expression`
  - [x] `IdentifierExpression`
  - [x] `LiteralExpression`
  - [x] `BinaryExpression`
  - [x] `UnaryExpression` if needed
- [x] Represent qualified names, for example `users.id`
- [x] Keep AST nodes immutable where practical

### Acceptance Criteria

- [x] AST can represent `SELECT id, name FROM users`
- [x] AST can represent `SELECT id FROM users WHERE age > 18`
- [x] AST can represent nested expression grouping such as `(age + 1) > 18`
- [x] AST contains no binder, catalog, heap, or execution dependencies

### Suggested Labels

- `query-engine`
- `parser`
- `ast`

---

## Issue 3: Add Parser for Initial SELECT Subset

### Goal

Build a hand-written parser that converts lexer tokens into the AST model.

### Scope

- [x] Add `Parser`
- [x] Parse `SELECT` statements
- [x] Parse comma-separated select lists
- [x] Parse one table in `FROM`
- [x] Parse optional `WHERE`
- [x] Parse identifiers and qualified identifiers
- [x] Parse literals
- [x] Parse expression parentheses
- [x] Parse comparison operators
- [x] Parse boolean `AND` / `OR`
- [x] Add Pratt-style expression parsing or equivalent precedence handling
- [x] Produce useful parse errors with source positions

### Initial Grammar Sketch

```text
statement      := select_statement
select_stmt    := SELECT select_item (, select_item)* FROM table_ref where_clause?
where_clause   := WHERE expression
table_ref      := identifier
select_item    := expression
expression     := parsed with precedence
```

### Acceptance Criteria

- [x] Parser accepts `SELECT id FROM users`
- [x] Parser accepts `SELECT id, name FROM users WHERE age > 18`
- [x] Parser handles expression precedence correctly
- [x] Parser rejects invalid SQL with clear errors
- [x] Unit tests cover successful parsing and parse failures

### Suggested Labels

- `query-engine`
- `parser`
- `frontend`

---

## Issue 4: Add Binder and Name Resolution

### Goal

Resolve SQL names against the system catalog and convert syntactic AST references into bound table and column references.

### Scope

- [x] Add `com.akita.query.bind` package
- [x] Add binder entry point that consumes:
  - [x] AST `Statement`
  - [x] `Catalog`
- [x] Add bound statement or bound query model
- [x] Add bound expression model:
  - [x] bound column references
  - [x] bound literals
  - [x] bound binary expressions
- [x] Resolve table names through `Catalog`
- [x] Resolve column names through `TableMetadata.schema()`
- [x] Track column ordinal position from `ColumnMetadata`
- [x] Attach `AkitaType` to bound expressions where possible
- [x] Reject missing tables
- [x] Reject missing columns
- [ ] Reject ambiguous column references once aliases or multiple scopes are introduced
- [x] Validate that `WHERE` resolves to a boolean expression
- [x] Add a simple binding scope abstraction even if only one scope is used initially

### Acceptance Criteria

- [x] Binder resolves `SELECT id FROM users`
- [x] Binder resolves `SELECT id FROM users WHERE age > 18`
- [x] Binder reports a clear error for unknown table names
- [x] Binder reports a clear error for unknown column names
- [x] Binder output is independent of the physical executor
- [x] Unit tests use a small in-memory catalog or test catalog

### Suggested Labels

- `query-engine`
- `binder`
- `catalog`

---

## Issue 5: Add Logical Plan Nodes

### Goal

Create the initial logical plan representation produced by the binder or by a binder-to-plan step.

### Scope

- [x] Add `com.akita.query.logical` package
- [x] Add `LogicalPlan`
- [x] Add `LogicalScan`
- [x] Add `LogicalFilter`
- [x] Add `LogicalProjection`
- [x] Represent output schema for logical plans
- [x] Store bound expressions in filter and projection nodes
- [x] Add a debug printer for logical plans

### Initial Plan Shape

```text
Projection
  Filter
    Scan
```

For a query without `WHERE`:

```text
Projection
  Scan
```

### Acceptance Criteria

- [x] Binder or planner can produce a logical scan for `FROM users`
- [x] Binder or planner can produce a logical filter for `WHERE age > 18`
- [x] Binder or planner can produce a logical projection for `SELECT id, name`
- [x] Unit tests assert the logical plan shape for basic queries

### Suggested Labels

- `query-engine`
- `logical-plan`

---

## Issue 6: Add Trivial Optimizer / Physical Planner

### Goal

Create an optimizer boundary that converts logical plans into physical plans. For milestone 1, this can be a simple passthrough planner with no real cost model.

### Scope

- [x] Add `com.akita.query.optimizer` package
- [x] Add optimizer or physical planner entry point
- [x] Add `com.akita.query.physical` package
- [x] Add `PhysicalPlan`
- [x] Add `SeqScanPlan`
- [x] Add `FilterPlan`
- [x] Add `ProjectionPlan`
- [x] Convert:
  - [x] `LogicalScan` to `SeqScanPlan`
  - [x] `LogicalFilter` to `FilterPlan`
  - [x] `LogicalProjection` to `ProjectionPlan`
- [x] Keep hooks open for future rule-based and cost-based optimization

### Acceptance Criteria

- [x] Logical plan can be converted to physical plan
- [x] Physical plan preserves table metadata, output schema, and bound expressions
- [x] Unit tests assert physical plan shape for basic queries

### Suggested Labels

- `query-engine`
- `optimizer`
- `physical-plan`

---

## Issue 7: Add Runtime Row and Expression Evaluation

### Goal

Introduce a schema-aware runtime row representation and expression evaluator for executor operators.

### Scope

- [x] Add `com.akita.query.execution` package
- [x] Add `Row`
- [ ] Decide whether `Row` stores:
  - [ ] `List<AkitaValue>`
  - [x] array of `AkitaValue`
  - [ ] another compact representation
- [x] Add expression evaluator for bound expressions
- [x] Evaluate bound column references by ordinal position
- [x] Evaluate literals
- [x] Evaluate comparison operators
- [x] Evaluate boolean `AND` / `OR`
- [x] Define null behavior or explicitly reject null expressions for milestone 1
- [x] Add schema-aware tuple decoder:
  - [x] `Tuple` + `Schema` -> `Row`
- [x] Add schema-aware tuple encoder if needed for tests:
  - [x] `Row` + `Schema` -> `Tuple`

### Acceptance Criteria

- [x] Can decode a heap tuple into a row using schema metadata
- [x] Can evaluate `age > 18` against a row
- [x] Can evaluate projection expressions against a row
- [x] Unit tests cover row decoding and expression evaluation

### Suggested Labels

- `query-engine`
- `executor`
- `datatype`

---

## Issue 8: Add Executor Operators

### Goal

Implement the initial physical operators needed for single-table reads.

### Scope

- [x] Add executor interface
- [x] Add `ExecutionContext`
- [x] Add `SeqScanExecutor`
- [x] Add `FilterExecutor`
- [x] Add `ProjectionExecutor`
- [ ] Decide executor iteration API:
  - [x] `Optional<Row> next()`
  - [ ] or `boolean hasNext()` plus `Row next()`
- [x] Open heap files from table metadata
- [x] Iterate records in a heap file
- [x] Decode heap tuples using table schema
- [x] Apply filter predicate
- [x] Apply projection

### Known Lower-Layer Gap

The heap layer may need an explicit scan/iterator API. If one does not exist yet, add the smallest API needed by `SeqScanExecutor`.

### Acceptance Criteria

- [x] `SeqScanExecutor` can return all rows from one table
- [x] `FilterExecutor` can skip rows that do not match a predicate
- [x] `ProjectionExecutor` returns only requested columns
- [x] Unit tests cover each executor independently

### Suggested Labels

- `query-engine`
- `executor`
- `heap`

---

## Issue 9: Add Query Facade for End-to-End Execution

### Goal

Add a small API that wires the query pipeline together for tests and early manual usage.
This API is also the future integration point for a CLI and PostgreSQL-compatible surfaces.

### Scope

- [x] Add query engine facade or service
- [x] Accept SQL string input
- [x] Run lexer/parser
- [x] Run binder
- [x] Build logical plan
- [x] Build physical plan
- [x] Create executor tree
- [x] Return result schema and rows
- [x] Surface parse, bind, and execution errors cleanly

### Possible API Sketch

```java
QueryResult result = queryEngine.execute("SELECT id, name FROM users WHERE age > 18");
List<Row> rows = result.rows();
```

### Acceptance Criteria

- [ ] End-to-end query returns expected rows for a populated heap table
- [ ] Unknown table error is surfaced clearly
- [ ] Unknown column error is surfaced clearly
- [ ] Parse error is surfaced clearly

### Suggested Labels

- `query-engine`
- `api`
- `integration`

---

## Issue 9a: Add Initial Query CLI

### Goal

Expose the query facade through a small command-line entry point for manual experimentation.

The CLI should depend on the query facade rather than wiring parser, binder, planner, or executor components itself.

### Scope

- [ ] Add a command entry point for executing one SQL string
- [ ] Print column names and rows in a readable format
- [ ] Convert query facade errors into clear terminal messages
- [ ] Keep catalog/storage bootstrapping explicit and simple for early usage
- [ ] Document the temporary limitations

### Possible Usage Sketch

```bash
akita "SELECT id, name FROM users WHERE age > 18;"
```

### Suggested Labels

- `query-engine`
- `cli`
- `developer-experience`

---

## Issue 10: Add End-to-End Single-Table SELECT Test

### Goal

Prove the first complete vertical slice works through real Akita components.

### Scope

- [ ] Create a test table schema
- [ ] Register table metadata in a catalog
- [ ] Create or open a heap file for the table
- [ ] Insert several tuples
- [ ] Execute:

```sql
SELECT id, name FROM users WHERE age > 18;
```

- [ ] Assert only matching rows are returned
- [ ] Assert projected columns are correct
- [ ] Assert non-projected columns are not returned

### Acceptance Criteria

- [ ] Test exercises parser, binder, planner, optimizer boundary, executor, heap, buffer pool, and catalog together
- [ ] Test passes reliably
- [ ] Test documents any temporary limitations

### Suggested Labels

- `query-engine`
- `integration-test`

---

## Follow-Up Milestones

These should become separate issues only after milestone 1 is working.

### Parser / SQL Surface

- [ ] `SELECT *`
- [ ] table aliases
- [ ] column aliases
- [ ] decimal and double literals
- [ ] unary operators
- [ ] `IS NULL`
- [ ] `LIKE`
- [ ] `ORDER BY`
- [ ] `LIMIT`

### Binder

- [ ] Multiple tables in scope
- [ ] Ambiguous column detection
- [ ] Table aliases
- [ ] Qualified column names
- [ ] Nested scopes
- [ ] Non-correlated subqueries
- [ ] Correlated subqueries

### Logical Planning

- [ ] Joins
- [ ] Aggregates
- [ ] Sort
- [ ] Limit
- [ ] Insert logical plan
- [ ] Update logical plan
- [ ] Delete logical plan

### Optimization

- [ ] Predicate pushdown
- [ ] Projection pushdown
- [ ] Constant folding
- [ ] Index scan selection
- [ ] Join reordering
- [ ] Table statistics
- [ ] Cost model

### Execution

- [ ] Nested loop join executor
- [ ] Index scan executor
- [ ] Sort executor
- [ ] Limit executor
- [ ] Aggregate executor
- [ ] Insert executor
- [ ] Update executor
- [ ] Delete executor

### Storage / Catalog Gaps Likely To Surface

- [ ] Heap file scan API
- [ ] Heap file page allocation when no page has enough free space
- [ ] Schema-aware tuple serialization
- [ ] Schema-aware tuple deserialization
- [x] In-memory test catalog
- [x] Catalog persistence improvements
- [ ] Table statistics storage

### PostgreSQL Compatibility Track

These should stay interface-focused: Akita keeps its own internals while gradually exposing PostgreSQL-shaped entry points.

- [ ] Map Akita result metadata to PostgreSQL-style column descriptions
- [ ] Add PostgreSQL-friendly type names for supported Akita types
- [ ] Expand SQL compatibility where it helps `psql` workflows
- [ ] Add a simple server process around the query facade
- [ ] Implement a minimal PostgreSQL wire protocol startup/query flow
- [ ] Support simple query protocol enough for `psql` smoke tests
- [ ] Add protocol-level error responses backed by query facade errors
- [ ] Document supported and unsupported PostgreSQL behavior
