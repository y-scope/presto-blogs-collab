# Presto–CLP Connector: Knowledge Base

- **Status:** Working source of truth synchronized with the publication outline and draft
- **Prepared:** August 6, 2026; updated August 7, 2026
- **Purpose:** Preserve the technical, attribution, deployment, capability-status, and future-looking context behind the publication outline and draft.

**Working publication title:** *Petabyte-Scale Schema-Less Data, Columnar by Design: The CLP Connector for Presto Native*. The approved Uber figure—approximately 25 PB of raw logs per month—supports the scale claim. “Columnar by design” identifies the architectural distinction: CLP makes observed structure part of its storage model rather than adding independent columns only for paths selected into a separate shredding layout. This presents a different tradeoff without portraying Parquet shredding as undesirable. The shorter conceptual alternate is *Schema-Less, Yet Columnar at Petabyte Scale: The CLP Connector for Presto Native*.

> This document is intentionally more detailed than the eventual article. It distinguishes publicly documented behavior, Uber production extensions, informed architectural interpretation, and facts that still require verification.

---

## 1. Executive Summary

- CLP, the Compressed Log Processor, is an open-source system for losslessly compressing and directly searching logs and other evolving semi-structured records. Logs are its strongest demonstrated use case because schemas recur, text contains repeated patterns, and producers change independently at high volume.
- A CLP archive stores many schema-homogeneous **encoded record tables (ERTs)** behind one schema tree. Every observed schema node is columnized inside its ERT; there is no global sparse union, Variant remainder, or writer-maintained promotion list.
- CLP also separates recurring text inside string values into logtypes and typed variables. Schema grouping and logtype extraction are independent compression and search advantages.
- Iceberg v3 Variant can also preserve schema-less data. Unshredded Parquet Variant keeps each dynamic value together in a native binary document column; optional shredding gives writer-selected paths independent typed columns. CLP's distinction is intrinsic columnization of every observed path within recurring schemas.
- The workload boundary matters. With one stable schema, CLP has no field-columnization advantage over typed Iceberg/Parquet. With many recurring, uncontrolled schemas, CLP avoids a global union or promotion policy. With almost one schema per event, neither design can amortize recurring record structure, although CLP may still find repeated logtypes.
- An optional convenience schema exposes selected path-and-type bindings as ordinary SQL columns after archives are written. It is late-bound projection over columns that already exist, not a physical rewrite.
- Current PrestoDB has `JSON`, fixed-shape `ROW`, and homogeneous `ARRAY(T)`, but no first-class `VARIANT`. The public connector therefore provides planner-typed exact-path accessors, typed wildcard value search across fields, and complete-event JSON reconstruction.
- These functions are plan markers rather than row-at-a-time JSON evaluators. The execution path is **query-plan resolution → projection/predicate pushdown → split-local schema resolution → native encoded execution**.
- Future PrestoDB Variant support could expose one logical `event VARIANT` over unchanged CLP storage. Schema-tree-backed wildcard key discovery and `{path, value}` projection are separate future directions; they are not current public APIs.
- The open-source connector supports searchable KV-IR files and immutable CLP archives, including mixed queries across both formats. The detailed ERT, schema-tree, and sequential object-storage mechanics describe immutable archive splits.
- The Presto Native worker supplies compute and loads the CLP connector plugin in-process. For an immutable archive split, the plugin links CLP as a shared library, opens the object-storage byte stream, and invokes CLP's streaming archive reader to interpret and search bytes as they arrive.
- Scale comes from avoiding work: metadata prunes archives, schema resolution eliminates ERTs and columns, dictionaries reject some predicates, CLP scans encoded values, and the archive layout supports forward sequential object-store reads. Uber additionally uses planning-time early termination for eligible newest-first limited queries.
- The open-source connector works end to end today over KV-IR and archive data. At Uber, the design serves workloads producing approximately 25 PB of raw logs per month; advanced metadata planning, early termination, and billion-file metadata scale remain production extensions.
- The connector is also an early reference for independently developed Presto Native plugins. Its implementation helped complete dynamic connector registration and connector-specific coordinator-to-worker serialization paths.
- YScope engineers led the connector implementation. IBM engineers and the Presto community supported infrastructure and review; YScope and Uber engineers extended and applied the architecture in production.
- The article should teach two things: why CLP's storage model fits recurring but uncontrolled schemas, and how another team can preserve a specialized native engine behind a Presto connector.

### Central thesis

> CLP brings columnar-by-design schema-less storage to Presto and demonstrates a practical path for independently developed native connectors.

### Supporting formulations

- The schema is a convenience and an optimization, not a boundary around the data.
- Common fields provide familiar SQL; schema-less functions unlock the long tail.
- The UDF is the SQL surface, query rewriting carries the intent, and CLP performs the execution.
- Metadata avoids work; CLP makes the remaining work inexpensive.
- CLP is not merely another file reader. It brings a specialized native storage and search engine into Presto Native.
- One logical document can map to many native columns.
- Compression does more than reduce bytes: its compact structure can guide queries.

---

## 2. Terminology

### CLP

- Working name in this article for the newer-generation semi-structured/schema-aware CLP storage and search engine and its surrounding connector context.
- Handles JSON and other semi-structured events whose field sets, nesting, and types may vary.
- Uses structural representations, typed value encodings, logtype information, and archive metadata.
- Searches its encoded representation rather than requiring a full raw JSON reconstruction before evaluation.
- Integrates through a native shared library; the article does not depend on the underlying native implementation language.
- Main repository: https://github.com/y-scope/clp

### μSlope

- Research system/paper underlying important CLP techniques:
  - **μSlope: High Compression and Fast Search on Semi-Structured Logs**
  - OSDI '24
  - https://www.usenix.org/conference/osdi24/presentation/wang-rui
- Joint work by authors affiliated with YScope, the University of Toronto, and Uber.
- Uber participation spanned the Observability, Pinot, and Storage and Search organizations.
- The paper is the correct deep technical reference for compression format, compact schema representation, and encoded search.

### Semi-structured logs

- Events contain recognizable keys, paths, arrays, and nested objects.
- No single centrally enforced schema describes every event across producers, versions, platforms, and time.
- New fields may appear immediately after an application deployment.
- Fields may disappear or move.
- Nesting may evolve.
- Field names may not map cleanly to SQL identifiers.

### Polymorphism

- The same logical field path may hold different types in different events.

Example:

~~~json
{"request_id": 12345}
{"request_id": "retry-12345"}
{"request_id": {"source": "gateway", "id": 12345}}
~~~

- This is normal in independently developed, continuously deployed systems.
- It conflicts with a SQL planner's need to assign a type to each expression.
- CLP should preserve the variants rather than silently coerce or discard them.

Polymorphism is recursive rather than limited to scalar object fields:

~~~json
{
  "outcome": [200, "retry", true, {"code": 503}, [42, null]]
}
~~~

- CLP arrays do not require every element to have the same type.
- An array element may itself be an object or another heterogeneous array.
- Object values may likewise be scalars, arrays, or objects with independently evolving structure.
- This cannot be represented faithfully as a conventional Presto `ARRAY(T)` unless `T` is itself a type-erasing envelope such as `JSON` or a future `VARIANT`.

### Variant

- Presto 0.298.1 does not currently document a first-class `VARIANT` data type comparable to Parquet Variant or Dremio Variant.
- This is a PrestoDB-specific statement. Trino 483 has a first-class `VARIANT` type that follows the Apache Iceberg Variant specification, including its type system, value encoding, and value semantics.
- Trino maps Iceberg `VARIANT` to Trino `VARIANT`, but only for Iceberg format-version 3 tables. Trino currently describes its Iceberg v3 support as experimental.
- The closest built-in type is `JSON`, with functions such as `json_extract` and `json_extract_scalar`.
- `JSON` preserves JSON-level structure, but the planner sees one `JSON` expression type; typed scalar use still requires extraction and casting.
- Parquet Variant is recursive:
  - a primitive contains a type and value;
  - an array is an ordered list of Variant values;
  - an object is a collection of string/Variant pairs.
- Consequently, one Variant value can preserve nested objects, mixed-type arrays, and per-event type differences without forcing all values into strings.
- Dremio's `VARIANT_GET` illustrates two useful forms:
  - exact path to `VARIANT`, preserving the runtime type;
  - exact path with `AS type`, producing a planner-known scalar or array type.
- Dremio's documented path syntax does not support wildcard paths. This illustrates an important distinction: Variant addresses **type uncertainty at a known path**, not **key uncertainty**.

### Schema-aware access

- Automatically discovered, common, or intentionally exposed fields behave like familiar SQL columns.
- Users can apply normal predicates, projection, grouping, joins, ordering, and aggregation.
- The user does not manually define the schema; CLP derives structural information from ingestion.

### Schema-less access

- Functions address data outside the conventional planned column surface.
- **Current public operations:** planner-typed exact-path access, typed wildcard value search across fields, and complete-event JSON reconstruction.
- **Future directions:** type-agnostic Variant access, wildcard or regular-expression key matching, and projection of matching `{path, value}` pairs.
- Schema-aware and schema-less access can appear in the same query.

### Presto Native / Presto C++

- The native C++ worker implementation of Presto, built using Velox.
- The Java coordinator still parses, analyzes, plans, and schedules the query.
- Native workers execute the plan.
- Preferred article terminology: **Presto Native** or **Presto-native worker**.
- Avoid calling this a “Velox plugin”; the integration is a Presto-native connector/plugin that uses the Presto/Velox execution environment.

### Out-of-band connector

- A connector developed, built, packaged, and released outside the core Presto repository.
- Loaded dynamically into a compatible Presto-native worker.
- Maintains its own source repository and release lifecycle.
- Still participates in the standard catalog, metadata, split, and execution model.

---

## 3. CLP Primer

### 3.1 What CLP compresses

CLP supports a continuum:

1. **Message-oriented logs**
   - Meaning primarily resides in natural language.
   - Variable values are embedded in repeated static text.

2. **Enriched application logs**
   - A natural-language message is retained.
   - A JSON envelope adds timestamp, service, host, job, correlation, and other context.

3. **Native JSON event logs**
   - Meaning primarily resides in fields describing requests, state, inputs, and outcomes.
   - Natural-language descriptions may still appear inside message-like fields.

The line between “structured” and “unstructured” is therefore blurred:

- A text message can live inside a structured envelope.
- A JSON event may contain essential natural language.
- JSON is an encoding syntax, not a promise of a stable schema.

### 3.2 Compression intuition

Repeated log messages often differ only in a few variables:

~~~text
Request 123 failed after 45 ms
Request 987 failed after 12 ms
~~~

A compact representation stores the recurring structure once:

~~~text
Request <int> failed after <int> ms
~~~

This is conceptual notation, not literal on-disk syntax. CLP records the variable types and stores the request IDs and durations separately using integer-appropriate encodings.

For JSON, keys, paths, nesting, and structural patterns also repeat within an archive. CLP can:

- Represent recurring structure compactly.
- Avoid repeating every key in every event.
- Separate structure from values.
- Preserve observed types.
- Group or organize events with related structures.
- Retain sufficient metadata to navigate and search that split's encoded data.

### 3.3 Structural scope is per split

- CLP does not need a single global schema tree covering the dataset.
- Each split carries or references the structure needed to interpret its own events.
- Different splits can contain different fields, nesting, and type variants.
- Within an event, objects and arrays may also contain recursively nested values of different types; CLP does not impose SQL's homogeneous `ARRAY(T)` model on the stored data.
- A new structure becomes queryable with a new split rather than waiting for a global schema migration.
- A table may still expose selected common fields for familiar SQL access.
- Schema-less functions reach beyond those fields using each split's local structural information.

### 3.4 Searchable compression

CLP's output is not intended to be an opaque compression blob that must be fully expanded before use.

The search engine can use:

- Schema-tree information
- Logtype dictionaries
- Encoded variables
- Typed values
- Timestamp bounds
- Archive-level statistics

to identify likely matches and selectively reconstruct the required values.

### 3.5 Why native in-process integration matters

- Each Presto Native worker loads the CLP connector plugin in-process; the plugin links CLP through a shared-library boundary.
- The worker supplies compute, while CLP's archive reader performs storage-specific interpretation, search, and projection.
- CLP's underlying implementation language may evolve without changing the query, pushdown, or connector model.
- Direct integration avoids:
  - A separate CLP query service in the scan path
  - Additional RPC transport
  - Per-record language-bridge calls
  - Reimplementation of the search engine in Java
  - Loss of CLP's encoded execution model

This makes CLP a better candidate for a true native connector than a remote, network-bound database where protocol and network costs dominate.

---

## 4. The Familiar Hive/Iceberg Mental Model

Presto community readers are likely familiar with Hive and Iceberg. Use that model to reduce cognitive load.

### 4.1 What is similar

- A Presto catalog exposes datasets as tables.
- The coordinator asks the connector for metadata.
- The connector plans units of work as splits.
- Metadata eliminates irrelevant files or archives.
- Workers scan splits in parallel.
- Filters and projections can be pushed down.
- Data can remain in a disaggregated filesystem or object store.

### 4.2 Approximate concept mapping

| Hive or Iceberg | CLP |
|---|---|
| Hive Metastore or Iceberg catalog | CLP metadata provider |
| Registered table schema | Selected common fields plus split-local structure and schema-less access |
| Partitions, manifests, file statistics | Archive metadata, time bounds, selected dimensions, optional global field view |
| Parquet or ORC file | CLP archive |
| File or row-group split | CLP archive split |
| Parquet or ORC reader | Native CLP search and projection engine |
| Partition/manifest/file pruning | Metadata-level archive pruning |
| Column predicate/projection pushdown | Column and schema-less operation pushdown |

### 4.3 Where the analogy breaks

#### No schema contract is required

- Hive and Iceberg normally begin with a schema registered in a catalog.
- CLP does not require producers or administrators to supply one.
- CLP maintains the structural knowledge required by each split internally.
- That knowledge can improve conventional SQL ergonomics without becoming a prerequisite.
- The connector need not union every field from every split into one global schema before querying.

#### The catalog schema is not the full query surface

- In Hive/Iceberg, columns largely define what can be referenced.
- In CLP, ordinary exposed fields coexist with dynamic path, key, value, type, and complete-event access.

#### The reader understands log structure

- CLP is not merely a generic columnar reader.
- It understands repeated log patterns, encoded variables, schema structures, and log-specific search semantics.

#### Read-only, append-oriented workload

- The public CLP connector is read-only.
- It is optimized for continuously arriving, immutable log archives.
- Hive and Iceberg support broader table lifecycle and write semantics.

#### Search semantics differ

- SQL LIKE can match values in a known column.
- It does not wildcard-match unknown field names or search every value without enumerating columns.
- CLP supports grep-like log expectations and dynamic key/value operations.

---

## 5. Why Standard SQL Is Not Enough

### 5.1 Planning contract

Presto normally needs:

- Column names
- Expression types
- Relation shape

during planning.

It cannot plan a conventional projection over arbitrary fields that may appear only at runtime.

This is an operational boundary as well as a language boundary:

- Application teams introduce, remove, rename, and change fields through ordinary deployments.
- These changes may occur without a catalog migration or coordination with Presto operators.
- A field can change type between producers, versions, splits, or events.
- A single array can contain values of several types.
- Nevertheless, each Presto expression must still have a stable result type at planning time.

### 5.2 Operations not naturally expressible in standard relational SQL

- Match field names using a wildcard or regular expression.
- Search a value without knowing which field contains it.
- Bind a dynamically matched key to its corresponding value.
- Project a field whose name was not part of the planned schema.
- Return a complete log event whose structure is not represented by the relation.
- Ask for one observed type of a polymorphic path.
- Search all type variants while preserving their distinctions.
- Return a known path without choosing one of its possible runtime types.
- Faithfully project a heterogeneous, recursively nested array through conventional `ARRAY(T)`.
- Preserve exact path semantics for keys containing dots, spaces, symbols, or case distinctions.

### 5.3 Why an opaque JSON column is inadequate

Putting each event in one JSON string preserves data but:

- Moves structural interpretation to every query.
- Encourages complete-document parsing.
- Materializes fields the query does not need.
- Weakens projection and predicate pushdown.
- Makes types and polymorphism the user's responsibility.
- Does not naturally solve wildcard field-name projection.
- Makes complete-event and dynamic-field operations vendor-specific JSON processing rather than native storage operations.

JSON functions can approximate some tasks, but they are database-specific extensions and typically operate on an opaque document value. They do not by themselves preserve CLP's compressed execution.

JSON remains a useful compatibility representation at the query boundary. The distinction is between storing and repeatedly parsing every event as an opaque JSON document versus asking CLP to evaluate an operation against its encoded representation and serialize only the selected value or matching complete event.

### 5.4 CLP's composable answer

#### Schema-aware path

- Common automatically discovered fields are addressable through normal SQL.
- Familiar to existing Presto users.
- Appropriate for timestamps, service identifiers, severity, host, job, and other frequent dimensions.

#### Schema-less path

- Typed or generic CLP functions express dynamic access.
- The function's declared result gives Presto a planning-time output type.
- CLP resolves the dynamic path or search semantics at execution.

#### Same-query composition

Typical investigation:

1. Restrict a time range using an ordinary column.
2. Filter service or platform using an ordinary column.
3. Search for a key matching a pattern.
4. Search or project its value.
5. Select a particular type if the field is polymorphic.
6. Aggregate, order, limit, or join using ordinary SQL.

### 5.5 Functions as query mechanics

Verified in the public connector source on August 7, 2026:

- `CLP_GET_BIGINT(path)`
- `CLP_GET_DOUBLE(path)`
- `CLP_GET_STRING(path)`
- `CLP_GET_BOOL(path)`
- `CLP_GET_STRING_ARRAY(path)`
- `CLP_GET_JSON_STRING()`
- `CLP_WILDCARD_STRING_COLUMN()`
- `CLP_WILDCARD_INT_COLUMN()`
- `CLP_WILDCARD_FLOAT_COLUMN()`
- `CLP_WILDCARD_BOOL_COLUMN()`

Important:

- Public tests demonstrate constant paths such as `user_id` and `city.Name`.
- `CLP_GET_JSON_STRING()` projects the complete event in JSON form.
- The typed wildcard functions are filter expressions for value searches across fields.
- Public tests cover equality, one-sided LIKE patterns, numeric comparisons, BETWEEN, IN, and logical combinations. Do not assume two-sided `%value%` pushdown without release-specific verification.
- A function can be used in SELECT, WHERE, or both.
- “Search UDF” and “projection UDF” are not necessarily separate categories.
- The registered Java functions are planner placeholders. The connector rewrites them into scan-level requests rather than executing them as record-at-a-time JSON parsers.

### 5.6 Query-plan resolution and pushdown

Use this four-stage vocabulary consistently:

> **Query-plan resolution → projection/predicate pushdown → split-local schema resolution → native encoded execution**

These are execution phases, not capabilities unique to CLP. Trino with Iceberg/Parquet also plans expressions, pushes supported work into a connector, resolves file layout, and executes in a reader. The differentiator is **what the execution phases can bind to and when that physical choice was made**:

| Execution phase | Trino + Iceberg/Parquet Variant | Presto + CLP |
|---|---|---|
| Query-plan resolution | Resolve Variant subscripts, casts, projections, and predicates | Recognize `CLP_GET_*` today—or future `event[...]`—and retain the logical path, requested type, and operation |
| Projection/predicate pushdown | Pass supported Variant subfields and filters toward the Iceberg/Parquet scan | Encode schema-less paths and compatible filters directly in CLP scan requests |
| Physical path resolution | If the path was shredded at write time, bind to its typed Parquet column; otherwise navigate within the unshredded binary Variant value | Match the path against each split's schema tree and bind it to all relevant schema/type-specific ERT columns already created at ingestion |
| Reader execution | Read shredded columns independently, or read/decompress pages containing co-located unshredded Variant values | Search and project native encoded ERT columns, with schema, dictionary, logtype, and variable-aware execution |

The timeline is the compact distinction:

> - **Parquet:** choose physical path independence at write time, then exploit it at query time.
> - **CLP:** columnize every observed schema path at ingestion, then bind any requested path to those columns at query time.

Implemented mechanism:

1. Presto parses and type-checks the SQL expression.
2. During **query-plan resolution**, the connector recognizes supported CLP functions and predicates and extracts their constant paths, requested types, and operations.
3. **Projection and predicate pushdown** translate that intent into connector-specific scan handles rather than row-at-a-time UDF evaluation.
4. The handles reach the native worker, where **split-local schema resolution** maps logical paths to the matching ERTs and physical columns.
5. CLP performs **native encoded execution** of compatible filtering and projection against those columns.
6. Unsupported or residual expressions remain in Presto.

Correctness rule:

- Partial pushdown must preserve a superset of the correct answer.
- An unsafe branch of OR cannot be dropped merely because the other branch is pushable.

### 5.7 Schema-aware ROW access versus complete-event JSON

Use “complete log event,” not “entire row,” in prose.

- ROW is a concrete Presto data type.
- Split-local structure is fundamental to each CLP archive.
- A derived global schema view is optional and may expose common nested objects through Presto `ROW`, with absent children represented as `NULL`.
- Pure schema-less UDF access does not require that a field appear in this global view.
- `CLP_GET_JSON_STRING()` returns a complete event in JSON form; it does not construct a dynamic Presto `ROW` containing every possible field.
- These schema-aware and schema-less paths coexist and should not be presented as conflicting implementations.

With future Variant support, this duality can have a simpler SQL surface:

- **Without a convenience schema:** expose one logical `event VARIANT` document column, potentially alongside fixed operational columns such as timestamp or archive metadata.
- **With a convenience schema:** expose selected path-and-type bindings as ordinary typed columns while retaining `event` as the escape hatch for everything else.
- Defining the convenience schema changes the logical table interface, not the CLP physical layout. The typed columns map to ERT columns that ingestion already created.
- A dereference such as `event['request']['id']` is defined to be recognized during planning and pushed into CLP as a path projection. The Variant surface is not release-ready until it reads the corresponding ERT columns without first reconstructing the complete event. Only projection of `event` itself requires complete-event reconstruction.

This yields the compact model:

> **One logical document, many native physical columns.** The convenience schema exposes chosen path-and-type bindings as conventional SQL columns; it does not cause a shredding rewrite.

### 5.8 Current type-agnostic access and a future Variant path

The typed-accessor design is a property of the **current PrestoDB connector boundary**, not a limitation of CLP's storage model. CLP already preserves the runtime types, nested objects, and heterogeneous arrays needed to construct a Variant value.

Current implemented surface:

- Typed accessors such as `CLP_GET_BIGINT(path)` give Presto a stable planning-time type.
- Different typed accessors can address different observed types at the same path.
- `CLP_GET_JSON_STRING()` is the currently verified type-agnostic projection for the **complete event**.
- Do not claim that the public connector currently provides `CLP_GET_JSON_STRING(path)` unless that overload is implemented and verified.
- Serialized JSON can faithfully represent JSON strings, numbers, booleans, nulls, nested objects, and heterogeneous arrays, but it does not preserve richer non-JSON type tags without an explicit serialization convention.

If Presto adds a native Variant type:

- Add a new type-agnostic accessor rather than changing an existing UDF's return type and breaking queries.
- Trino does not define a path-taking `variant(...)` getter. It exposes a Variant expression and uses standard SQL subscripts and casts: `variant_expression['key']`, `variant_expression[1]`, and `CAST(... AS type)`.
- The closest connector bridge is therefore a complete-event expression such as `CLP_VARIANT() -> VARIANT`, used as `CLP_VARIANT()['request']['id']`. Exposing an ordinary `event VARIANT` column would be even more portable because the same subscript/cast query shape could work over CLP and Iceberg tables.
- A convenience function such as `CLP_GET_VARIANT(path) -> VARIANT` could coexist, but it remains connector-specific and is less useful as a migration contract than a root Variant expression.
- Either form could preserve a scalar, object, or recursively heterogeneous array. The existing typed accessors remain useful for typed predicates, aggregation, and explicit pushdown.
- This requires Variant support across the engine boundary, not merely a connector signature: the coordinator type system and serialization, Presto Native/Velox representation, operators/casts, and connector Java/native mappings must agree on the value model.
- Once that engine support exists, mapping CLP's recursive value into Variant is a natural connector integration; CLP does not need to change its on-disk model.
- Do not publish `CLP_VARIANT()` today with a JSON return type and later change it to Variant; changing a function's return contract can break views, prepared queries, casts, and client assumptions. Reserve or introduce it when true Variant support is available.
- The exact name and signature are design ideas, not current public APIs.

Why Variant is not the complete schema-less answer:

- An exact-path getter still assumes the user knows the key.
- Field discovery, key-name matching, key-only search, and searches across unknown keys require additional operations.
- A wildcard expression may match zero, one, or many paths, so it should not be forced into an API whose typed form implies one scalar result.
- A future wildcard projection could return one nested Variant containing match records such as `{path, value}`, preserving both field identity and each value's runtime type.
- Until Variant exists, the compatibility form could be serialized JSON.
- Function names such as `CLP_GET_MATCHES(pattern)` are illustrative only; the current public connector should be described using its verified typed `CLP_WILDCARD_*_COLUMN()` value-search functions.

Potential execution model for future wildcard projection:

1. Require a constant path pattern so it can participate in planning and pushdown.
2. Match the pattern against each split's local schema tree.
3. Identify only encoded fields and types that could satisfy it.
4. Apply filtering without materializing a result collection when the query only needs existence or any-match semantics.
5. For projection, return the matched paths and values as Variant or serialized JSON.

This is a durable CLP distinction: Variant can preserve an unknown runtime type, while CLP's structural metadata can help find unknown fields efficiently.

### 5.9 Trino/Iceberg Variant compared with CLP

#### Direct answer: why not just use Iceberg?

You can. Where the query engine supports Iceberg v3 Variant, Iceberg can preserve schema-less records in an interoperable table. With one stable schema, CLP has no field-columnization advantage over ordinary typed Iceberg/Parquet because both can store every field as a column. Variant plus shredding can similarly provide columnar treatment for selected dynamic paths.

The distinction is not whether Iceberg can store schema-less data. It is whether every future nested path should receive field-level columnar execution without being predicted first. An unshredded Parquet Variant keeps one complete native binary value per row; shredding gives independent typed columns to paths and types chosen by the writer. New or unanticipated paths remain unshredded, and changing the physical selection for existing files requires rewriting or replacing them. Iceberg's table abstraction and schema evolution do not retroactively create those physical inner columns.

CLP discovers recurring key-and-type shapes and columnizes every observed schema node during ingestion, without a promotion list. This is most differentiated when schemas recur but are controlled by many independent producers, paths and types are unpredictable, cross-field value search matters, and repetitive log strings benefit from logtype/variable separation. It is not a universal performance claim: stable typed data may favor ordinary Iceberg/Parquet, while almost one unique schema per event deprives both designs of structural repetition.

**Research status:** Verified against Trino 483, the Apache Iceberg specification, and the Apache Parquet Variant specification on August 7, 2026.

The comparison requires separating three related layers:

1. **Trino `VARIANT`** is an engine-level SQL type with value semantics, exact-key and array-element access through the subscript operator, casts to and from conventional SQL types, and a way to distinguish Variant null from SQL `NULL`.
2. **Iceberg `variant`** is a semi-structured table-column type added in Iceberg v3. Its objects may have different fields and value types across rows, and its arrays may contain recursively heterogeneous values.
3. **Parquet Variant** defines the physical `metadata` and `value` encoding and optional **shredding**, which stores selected paths as typed Parquet columns.

CLP is broader than any one of these layers. It is a specialized compression and search engine with structural metadata, logtype dictionaries, typed encodings, archive statistics, and a connector-specific SQL surface. Variant primarily answers how one dynamic value should be represented and manipulated by an engine; CLP additionally answers how evolving log events should be compressed, discovered, searched, pruned, and selectively reconstructed.

#### Data-model similarities

Both models can preserve:

- Objects whose field sets vary by event.
- One path holding different types in different events.
- Arrays containing different scalar types, objects, nested arrays, and nulls.
- Exact nested values without coercing every value to a string.
- A combination of conventional stable columns and a dynamic long tail.

This makes Iceberg Variant a much closer comparison to CLP than an opaque JSON string, a fixed Presto `ROW`, or a homogeneous `ARRAY(T)`.

#### Current SQL-surface difference

Trino 483 documents exact object-key and numeric array-element access. Each subscript returns `VARIANT`, which the query can cast to a conventional SQL type:

~~~sql
SELECT
    CAST(event['request']['id'] AS VARCHAR) AS request_id,
    CAST(event['response']['status'] AS BIGINT) AS status
FROM iceberg.logs
WHERE CAST(event['response']['status'] AS BIGINT) >= 500;
~~~

The CLP public connector instead gives the planner a stable result type at the function boundary:

~~~sql
SELECT
    CLP_GET_STRING('request.id') AS request_id,
    CLP_GET_BIGINT('response.status') AS status
FROM clp.logs
WHERE CLP_GET_BIGINT('response.status') >= 500
  AND CLP_WILDCARD_STRING_COLUMN() LIKE 'timeout%';
~~~

Variant is ergonomically stronger than the **current public connector API** for type-agnostic exact-path projection: one expression can remain a scalar, object, or heterogeneous array until the user chooses to cast or serialize it. Today's API requires a typed accessor for an exact path; `CLP_GET_JSON_STRING()` is the verified type-agnostic escape hatch for the complete event, not for one arbitrary path. This is not inherent to CLP: with end-to-end PrestoDB Variant support, the connector could naturally expose a Variant-returning exact-path accessor.

CLP has a different advantage. Trino's documented Variant surface provides exact keys and indexes, but does not document a native wildcard-path, key-discovery, or search-across-all-fields operator. The current public CLP connector provides typed wildcard **value search across fields**. It does not yet provide general wildcard-field projection, so the article must not overstate that capability.

At the query layer, describe the two systems as **overlapping in user goals but different in abstraction**:

| Query operation | Trino `VARIANT` | Current public CLP connector |
|---|---|---|
| Read/filter a known exact path | Subscript a Variant value, then optionally cast it | Use the matching planner-typed `CLP_GET_*` accessor; supported predicates can be rewritten and pushed down |
| Select one type from a polymorphic path | Inspect/cast the runtime Variant value | Choose a typed accessor such as `CLP_GET_BIGINT(path)` or `CLP_GET_STRING(path)` |
| Preserve an arbitrary exact-path value without choosing its type | Yes: the result remains `VARIANT`, including objects and heterogeneous arrays | Not in the verified public API; once Variant exists end to end, expose a root `CLP_VARIANT()` or `event VARIANT` expression and use standard subscripts |
| Return the complete event | Preserve it natively as Variant | Reconstruct it through `CLP_GET_JSON_STRING()` |
| Search a value without knowing its field | No corresponding operator in Trino 483's documented Variant surface | Yes: typed `CLP_WILDCARD_*_COLUMN()` expressions search values across dynamically discovered fields |
| Match/discover wildcard key names or project all matching fields | Not documented | Not yet in the verified public connector; the CLP schema tree makes it a natural future capability |
| Apply log-specific encoded search | Not supplied by the Variant type itself | CLP can use schema, logtype, variable, dictionary, and archive metadata during native execution |

Therefore, “CLP has Variant-like query functionality plus wildcards” is directionally useful but too broad. Safer wording is:

> CLP's typed accessors cover many of the same known-path projection and filtering tasks as Trino Variant, while its wildcard functions additionally search values across fields whose names the query does not know. Trino retains the more general type-agnostic exact-path result; wildcard key discovery and projection remain future CLP work.

#### Why wildcard keys are physically different

Exact-path lookup and wildcard-key lookup create different storage work. In unshredded Parquet Variant, an object stores field IDs and offsets inside each binary Variant value. The encoding supports efficient lookup of a known key, including binary search over sorted field IDs, but an arbitrary key pattern such as `request.*.id` is not one independently addressable physical column. A reader must inspect the relevant Variant metadata and object structures to determine which fields are present; shredding only helps the paths selected by the writer in advance.

CLP instead records observed key-and-type paths in an archive-level schema tree. A wildcard key pattern can first be matched against that schema metadata, eliminating schemas that cannot match and identifying the ERT columns for those that can. Pure key discovery may be largely metadata work; a value predicate under matching keys then scans only the selected columns. This is the physical foundation for CLP's current cross-field value search and for future wildcard key discovery/projection.

Do not claim that this is the documented reason Trino 483 lacks a wildcard-key operator. Trino could define such an operator over Variant. The defensible distinction is that arbitrary key matching over unshredded Variant generally requires inspecting per-value object structure, whereas CLP can resolve candidate paths from archive-level schema metadata before scanning records.

#### Physical execution difference

Separate two kinds of work avoidance:

1. **Avoid materializing unrelated values.** Decode only the requested path rather than constructing the complete event.
2. **Avoid reading or scanning unrelated encoded data.** Give the requested path its own physical column or stream so unrelated field data can be skipped.

An unshredded Parquet Variant provides the first property more strongly than an opaque JSON string, but not necessarily the second. Parquet stores the Variant as `metadata` and `value` binary columns, with one logical Variant value per table row. Internal field dictionaries and offsets let a reader navigate directly to a path such as `A.B` without constructing sibling subtrees. However, `A.B` remains physically co-located with the other unshredded fields inside each Variant value. The reader generally fetches and decompresses Parquet pages containing those complete binary values even when it materializes only `A.B`, and the unshredded path does not have an independent typed column with its own statistics.

A useful mental model is that unshredded Parquet Variant behaves like a **native binary document column**. Like a JSON column, one row contains one complete dynamic document. Unlike textual JSON, Variant preserves native types in a compact binary representation and supports direct offset-based navigation, so it is neither text nor an opaque blob. The analogy describes physical co-location, not decoding quality.

Parquet Variant shredding adds the second property for selected paths. A writer extracts paths such as `A.B` into typed Parquet columns, enabling ordinary projection, encoding, statistics, and data skipping without reading the Variant remainder when it is unnecessary. Shredding is therefore powerful, but it is a write-time physical-layout choice. Different files may legally shred different paths or types, and rare or newly introduced paths may remain in the binary `value` representation.

Why not shred every observed path? Within a file, a shredding schema that covers the union of heterogeneous paths and typed variants becomes wide, while most records omit many of those fields. The [Parquet Variant shredding specification](https://parquet.apache.org/docs/file-format/types/variantshredding/) represents an absent shredded field by setting its `value` and `typed_value` to missing. Parquet's definition levels and column encodings can represent missingness compactly, so this should not be described as storing a dense explicit null for every row. The remaining costs are a wider physical schema, more column metadata and streams, weakly populated columns, and a shredding policy that must keep deciding which path/type combinations deserve independent treatment. The unshredded Variant `value` is the escape hatch for the dynamic long tail; optional selective shredding exploits the partially homogeneous portion of the data without forcing every possible field into one sparse union.

This is also why CLP's ERT grouping matters. CLP does not globally flatten the union of every observed field into one table. It groups records by identical key-and-type schema, then columnizes every node inside each homogeneous group. CLP can therefore obtain the “all fields shredded” effect without creating one dataset-wide sparse column for every path/type combination. If schemas rarely repeat, CLP coalesces small ERTs internally rather than retaining an unbounded number of physical singleton tables. Coalescing controls metadata/table overhead, but it cannot create structural repetition that is absent from the data.

#### Why a purpose-built multi-ERT archive rather than reusing Parquet

The [Parquet file layout](https://parquet.apache.org/docs/file-format/) organizes one set of `N` columns across `M` row groups; each row group contains a column chunk for the file's columns. In practical terms, one Parquet file has one physical schema shared by its row groups. This is well suited to one table-shaped dataset, including nested columns, but it is not a native container for hundreds or thousands of independently schema-homogeneous tables whose column sets and types differ.

There are two possible Parquet mappings, neither impossible but each with a tradeoff:

1. **One union schema:** flatten all observed path/type combinations into one wide table. Heterogeneous events then populate only a fraction of those columns, recreating the sparse-column problem that selective Variant shredding avoids.
2. **One file or table per schema:** encode each ERT using its own Parquet schema. This preserves homogeneity but converts schema diversity into many small files/objects, repeated file schemas and footers, more discovery/catalog entries, and additional planning and object-store requests. Larger batching can reduce these costs but cannot put different row-group schemas into one ordinary Parquet file.

CLP chooses a different unit: one archive contains many schema-specific ERTs behind one archive-level schema tree. The archive can package their dictionaries, indexes, logtypes, variable encodings, and encoded streams together, then route a requested path to the ERTs that contain it. The same unit supports the front-loaded metadata and forward sequential object-store access described later. This is a core reason for the purpose-built CLP format, alongside its specialized lossless log compression and direct encoded search.

Publication guardrail: do not say Parquet has literally “one table” in every conceptual sense or cannot represent the dataset. Say that one Parquet file has one shared physical schema across row groups, while a CLP archive is designed as a searchable container for many schema-specific ERTs. A Parquet dataset can approximate the latter with many files and catalog/manifest metadata, but the coordination moves outside the file and has different overheads.

Once a Parquet file is persisted, changing which paths are physically shredded requires writing a new physical representation—normally by rewriting or replacing the file. Changing a catalog schema alone cannot retroactively create those inner Parquet columns.

CLP makes comprehensive structurization intrinsic to ingestion rather than selecting a small promotion list. It builds an archive-level schema tree, stores each unique schema once, and groups events with the same schema into an **encoded record table (ERT)**. Each schema node has a corresponding ERT column, and leaf values use type-specific encodings. A query resolves a path or pattern against the schema tree to determine which schemas can match, then identifies the exact ERTs and columns that need to be scanned. Dictionaries for variable strings and logtypes can reject some string predicates before an ERT scan.

This grouping is not merely “one wide table with every possible field.” Each ERT is homogeneous: its records have the same keys and value types. The design avoids a globally sparse table while bringing relational-style columnar compression and scanning to each repeated schema. It also applies CLP's log-text encoding inside string fields, separating recurring message patterns from variable values. Thus CLP exploits repetition at two levels:

Grouping by identical schema is itself one contributor to compression. Every ERT column contains values with the same structural role and type, so type-specific encoders see more homogeneous input and do not spend capacity representing a dataset-wide sparse union. This can improve compression relative to general-purpose Parquet layouts for heterogeneous logs. It is not the only contributor: schema deduplication, specialized typed encodings, dictionaries, and logtype/variable separation also matter. Publication wording should compare physical compression with Parquet or Parquet-backed Iceberg, not Iceberg in isolation, because Iceberg supplies the table abstraction rather than the underlying value codec.

- **Event structure:** records with the same schema share one ERT and one stored schema description.
- **Structure inside string values:** similar natural-language values share a **logtype** containing the static text, while changing values are extracted, typed, and encoded as separate variables. For example, `Request 123 failed after 45 ms` and `Request 987 failed after 12 ms` can be described conceptually by `Request <int> failed after <int> ms`, with the request IDs and durations stored using integer-appropriate encodings. The typed placeholder notation is explanatory rather than literal on-disk syntax.

These are independent axes of structurization. Parquet shredding addresses the first axis by turning known document paths into physical columns. It does not normally destructure each natural-language string inside those columns into a reusable template and variables. CLP performs both schema/path columnization and log-message destructuring. Consequently, when every record has one stable schema, Parquet and CLP may converge on the first axis while CLP can still go further on the second.

It is reasonable to say that CLP achieves an **“all fields shredded” effect**, provided the article labels this as an analogy rather than the Parquet mechanism. There is no writer-maintained promotion list and no later rewrite that extracts paths from a Variant remainder. Instead, every node is already a native encoded column inside each schema-specific ERT. The same logical path can therefore map to columns in several ERTs—and possibly to different typed variants—rather than to one global physical column shared by the entire dataset.

This makes CLP's optional convenience schema analogous to a **post-write shredding policy at the SQL surface**:

1. CLP ingestion has already discovered every observed schema and columnized every node inside its ERT.
2. After the archive is persisted, an administrator or user can expose a different set of path-and-type bindings as ordinary typed SQL columns.
3. The connector remaps those logical columns to the ERT columns already on disk; it does not modify, rewrite, or re-encode the archive.
4. Path/type combinations absent from the convenience schema remain accessible through schema-less functions—or, with future Variant support, through `event[...]` plus an appropriate cast or type operation.

The safest wording is therefore **“late-bound projection over already-columnized storage,”** not “late shredding.” CLP needs no physical shredding mode because its schema-specific columnization is intrinsic. The user can change the apparent shredding policy after the write precisely because the storage layout does not depend on that policy.

The same applies when a path is not included in the convenience schema. During **query-plan resolution**, the current connector recognizes the constant path in a typed `CLP_GET_*(path)` call. Its UDF rewriter turns the expression into requested scan fields and performs **projection/predicate pushdown**; at the worker, split-local schema resolution maps the path to matching ERT columns for native encoded execution. It therefore receives **shredded-like column access without write-time declaration** and does not reconstruct a per-row Variant blob first. The future Variant implementation will lower constant expressions such as `event['a']['b']` through this same pipeline; pushdown is a release requirement, not optional follow-up work. Nothing in CLP's storage model blocks this because the target ERT columns already exist. This is a statement about path-level work avoidance, not guaranteed equality with Parquet's absolute performance. Dynamic per-row paths, unsupported residual predicates, wildcard matches spanning many columns, complete-event projection, multiple matching ERTs, and physical compression-stream packaging can change the amount of work.

This is why repetitive machine-generated logs are especially favorable. Schema matching can eliminate ERTs that cannot satisfy the query; dictionary search can reject absent values; and the remaining scan operates on homogeneous encoded columns. The important variable is not the raw number of distinct schemas but **records per schema**. A dataset can have many schemas and still benefit when each schema recurs enough to form a useful homogeneous ERT. The structural advantage weakens as schemas approach one-off occurrences. At the limit, nearly every event has a different shape; CLP coalesces the resulting small ERTs internally, so it does not simply accumulate one physical singleton table per event. The relevant design behavior converges with Parquet because neither representation can exploit recurring schema structure. Do not confuse this with every event having different values: rows with different values but the same keys and types still share a schema and an ERT. Low value or message-pattern repetition is a separate limitation that can reduce dictionary rejection and text-compression gains without defeating schema grouping.

The comparison is easiest to understand as three structural regimes:

| Workload regime | Parquet | CLP | Expected structural distinction |
|---|---|---|---|
| **One stable schema** | Ordinary typed Parquet already stores every leaf as a column; alternatively, a Variant can comprehensively shred the known paths. Natural-language fields remain string values compressed with general-purpose column encodings/codecs. | All records occupy one homogeneous ERT with a column per schema node; recurring messages can additionally be split into shared logtypes and separately encoded variables. | The layouts converge in schema/path columnarity, but not in value-level structurization. Automatic schema grouping is not the main differentiator here; CLP's log destructuring can remain one. |
| **Many recurring, uncontrolled schemas** | Unshredded Variant preserves the values but keeps inner paths co-located. Shredding accelerates paths and types anticipated by the writer; new, rare, or type-polymorphic cases may remain in the Variant remainder. | Ingestion discovers each observed key-and-type shape, creates an ERT for it, and columnizes all of its nodes automatically. | This is CLP's strongest expected relative case: it obtains path-level columnarity without requiring producers or table owners to coordinate a global schema or promotion list. The size of the advantage remains a benchmark question. |
| **Almost one schema per record** | Typed columns become impractical or extremely sparse, while unshredded Variant loses much of the benefit of cross-row columnar repetition. | CLP coalesces small ERTs internally to avoid one physical singleton table per schema, but the merged representation still has little recurring structure to exploit. | The designs converge with respect to schema repetition: neither has a structural-columnization advantage. Absolute performance may still differ because encodings and query engines differ. |

“Uncontrolled schema” is central for logs. The producer typically emits an event rather than creating and maintaining the analytical table that will eventually store it. Different services and versions can add fields, omit them, change nesting, or produce different types at the same path. A conventional Parquet pipeline must either enforce/coerce a shared typed schema, retain the data in an unshredded dynamic value, or continually choose which paths and types to shred. CLP instead treats the input as a black box at ingestion, discovers the observed schemas, and creates homogeneous physical columns within each schema group.

Do not overstate byte-level I/O isolation. CLP's logical layout identifies the exact ERTs and columns required and minimizes decompression and scanning, but the number of unrelated bytes physically fetched can still depend on how those columns are packaged into archive compression streams. Safe wording is “avoid scanning unrelated columns” or “minimize decompression and scan,” not “never read any unrelated byte.”

The concise distinction is:

> Unshredded Parquet Variant is columnar around the dynamic value but keeps its fields together inside a binary value. Shredding promotes selected inner paths to physical columns. CLP makes schema-specific structurization part of ingestion, creating an encoded table for every observed schema and a column for every schema node.

The article should not claim that Variant lacks pushdown. It should say that Variant enables optimized projection and filtering when the engine and physical layout exploit it, especially through shredding. Nor should it describe CLP as universally faster. Its strongest expected relative advantage is the middle regime—many recurring schemas that the storage-table owner cannot prescribe in advance—but the performance ordering must still be measured for the workload and query shape.

The article may make the workload-scoped qualitative claim that CLP typically achieves a substantially higher compression ratio than general-purpose Parquet on repetitive log data. Link to the [μSlope evaluation](https://www.usenix.org/system/files/osdi24-wang-rui.pdf) for broader compression evidence, while noting that Parquet was not one of its evaluated baselines. Attribute the advantage to schema deduplication, homogeneous ERT columns, and logtype/variable separation; do not generalize it to arbitrary semi-structured data. Any numerical multiplier—or query-performance ordering—requires a controlled comparison over the same losslessly represented corpus, query set, Parquet codec, shredding policy, and reader implementation.

#### Capability comparison

| Dimension | Trino with Iceberg Variant | CLP through Presto |
|---|---|---|
| Primary abstraction | Standard semi-structured SQL/table type | Specialized compressed storage and search engine |
| Exact-path result | `VARIANT`, then optionally cast | Planner-typed `CLP_GET_*` result |
| Complete dynamic value | Native `VARIANT` | Complete-event JSON through `CLP_GET_JSON_STRING()` |
| Heterogeneous arrays | Preserved natively | Preserved in CLP; current Presto surface is limited by available accessors |
| Unknown-key operations | No documented native wildcard-path or cross-field search operator in Trino 483 | Typed wildcard value search exists; general wildcard-field projection is future work |
| Physical acceleration | Binary path navigation when unshredded; typed columnar execution for selected shredded paths | ERT per observed schema, column per schema node, logtype encoding, and storage-native compressed search |
| Metadata model | Iceberg snapshots, manifests, partitions, and file metadata | Pluggable CLP metadata plus split-local structure and archive statistics |
| Table lifecycle | General lakehouse table semantics and multi-engine interoperability | Read-only, append-oriented public connector |
| Current maturity | Variant requires Iceberg v3; Trino's v3 support is experimental | Public connector is usable but specialized and has a smaller interoperability ecosystem |

#### Trino/Iceberg Variant advantages over CLP

- It is an open, interoperable type and encoding rather than a connector-specific query contract.
- It gives SQL one type-agnostic value for scalars, objects, and heterogeneous arrays.
- Exact-path access is more natural and avoids a separate accessor for every requested type.
- Iceberg provides general table capabilities such as snapshots, writes, schema evolution, and multi-engine access.
- Optional Parquet shredding lets deployments optimize predictable paths using familiar columnar techniques.

#### Trino/Iceberg Variant limitations relative to CLP

- The currently documented Trino interface addresses known keys; it does not provide CLP's typed wildcard value search across fields.
- Variant by itself does not supply log-pattern dictionaries, grep-like message semantics, or log-specific compressed search.
- Efficient partial reads and skipping depend on the physical layout, shredding choices, and reader implementation.
- As of Trino 483, Iceberg v3 support is experimental. The connector documentation says row-level updates, row-level deletes, and `OPTIMIZE` are not supported for v3 tables.
- Variant does not by itself reproduce CLP's production metadata planning or planning-time early termination.

#### CLP limitations relative to Trino/Iceberg Variant

- Current typed accessors require the user or query builder to select the desired runtime type.
- The verified public API lacks a type-agnostic exact-path result equivalent to `VARIANT`; this is a current engine/API gap, not a CLP storage limitation.
- CLP functions and their rewriting behavior are connector-specific rather than portable SQL semantics.
- The public connector is read-only and optimized for immutable, append-oriented datasets rather than general lakehouse table management.
- CLP has fewer engine integrations and a smaller interoperability ecosystem than Iceberg and Parquet.

#### Recommended article position

Do not frame Variant and CLP as mutually exclusive replacements. The durable distinction is:

> Variant answers, “What value and runtime type exist at this known path?” CLP additionally addresses, “How can the system discover and search evolving data while preserving log-specific compression and encoded execution?”

A future PrestoDB Variant would improve CLP's SQL boundary rather than eliminate the connector. Once the type is supported across the coordinator and native workers, CLP could naturally map its complete recursive event into `CLP_VARIANT()` or an `event VARIANT` column. Standard subscripts and casts would then match Trino's query shape, while typed accessors remain available for strongly typed predicates and explicit pushdown. CLP's structural metadata and wildcard value-search semantics would continue to solve a different problem.

Editorial guardrails:

- Say **PrestoDB** when stating that the engine discussed in this article lacks Variant; do not imply that Trino also lacks it.
- Do not say Variant inherently lacks pushdown. Separate the logical type from Parquet shredding and reader implementation.
- Describe the absence of wildcard operations as a limitation of Trino 483's documented interface, not an intrinsic impossibility for the Variant data model.
- Do not call Parquet Variant an opaque JSON blob; its binary encoding is self-describing and designed for path navigation.
- Do not claim general wildcard-field projection for the current public CLP connector. The verified capability is typed wildcard value search across fields.
- Keep any CLP-versus-Parquet compression or performance result tied to a controlled, reproducible comparison.

---

## 6. Presto-Native Connector Architecture

### 6.1 Logical components

#### SQL clients

- CLI
- Notebooks
- Dashboards
- Applications
- At Uber: observability UI, platform query builders, and programmatic clients

#### Java coordinator

- Parses and analyzes SQL.
- Resolves catalogs, tables, columns, and functions.
- Optimizes and schedules work.
- Hosts the coordinator-side connector responsibilities.
- Requests metadata and split information.

#### Coordinator-side CLP integration

- Discovers datasets, selected common fields, and split-local structural metadata.
- Maps CLP metadata into Presto catalog/table concepts.
- Translates supported top-level filters into metadata queries.
- Produces CLP-specific split and handle information.
- Exact ownership of UDF rewriting requires verification.

#### Presto-native worker

- Dynamically loads the external CLP connector plugin, which links CLP as a shared library.
- Receives plan fragments, splits, and connector-specific serialized values.
- Invokes CLP directly.
- Produces vectors/pages for downstream Presto operators.

#### Metadata provider

- Provides table metadata, selected common fields, and split-local structure metadata.
- Discovers searchable KV-IR files and immutable archives.
- Can produce KV-IR splits, archive splits, or a mixed split set.
- Produces or filters splits.
- May expose time ranges, selected dimensions, record counts, and other statistics.
- Interfaces allow implementations beyond the default open-source provider.

#### CLP storage splits

- Searchable KV-IR files and immutable CLP archives may be stored on a filesystem or object storage, depending on deployment.
- The connector dispatches the appropriate format reader for each split; one query may contain both formats.
- The detailed ERT, archive-local schema-tree, and forward object-storage access model in this knowledge base applies to immutable archive splits.
- In Uber production, each Presto Native worker is a compute process running the CLP connector in-process. For an archive split, the connector opens an object-storage byte stream, and CLP's streaming archive reader consumes, interprets, and searches it as archive bytes arrive.
- No local archive cache or local-disk staging is used in the described Uber query path.

### 6.2 Proposed high-level diagram

~~~mermaid
flowchart TB
    user["SQL clients"]

    subgraph presto["Presto"]
        coordinator["Java coordinator<br/>Parse · analyze · plan · schedule"]
        coordPlugin["CLP coordinator integration<br/>Metadata · split planning"]
        worker["Presto-native workers"]
        nativePlugin["In-process CLP connector plugin<br/>CLP shared library · search · filter · project"]
    end

    subgraph storage["CLP storage"]
        metadata["Pluggable metadata provider<br/>Splits · structure · statistics"]
        files["Searchable KV-IR<br/>Immutable CLP archives"]
    end

    user --> coordinator
    coordinator --> coordPlugin
    coordPlugin <--> metadata
    coordPlugin --> worker
    worker --> nativePlugin
    nativePlugin --> files
    nativePlugin --> worker
    worker --> coordinator
    coordinator --> user
~~~

### 6.3 Why shared-library integration matters

- Runs an existing native storage engine inside each worker process.
- Preserves CLP's native algorithms.
- Avoids a remote service boundary in the hot scan path.
- Lets the connector remain independently developed.
- Enables separate release and packaging.
- Requires careful compatibility management with Presto Native and Velox.

---

## 7. Enabling External Native Connectors

### 7.1 The infrastructure gap

Before this work, independently developed out-of-band native connectors were not yet a mature real-world Presto-native path.

Required capabilities included:

- Load a connector shared library outside the core binary.
- Register its connector factory.
- Avoid requiring connector source to be compiled into Presto.
- Route connector-specific serialized data from the Java coordinator.
- Deserialize custom handles on the native worker.
- Maintain a compatible contract across the Java coordinator and native workers.

### 7.2 Upstream changes

- https://github.com/prestodb/presto/pull/27653
  - “Hoist plugin loading and de-inline factory map for dynamic connector support.”
  - Enables dynamically loaded code to participate in connector registration.

- https://github.com/prestodb/presto/pull/27652
  - “Add customSerializedValue routing for binary connector deserialization.”
  - Provides a path for connector-specific serialized values to reach the native side.

### 7.3 Correct claim strength

Preferred:

- “An early reference implementation of an external Presto-native connector.”
- “One of Presto Native's first real out-of-band native connectors.”
- “Helped complete important missing infrastructure for external native connectors.”

Avoid until chronology is verified:

- “The first native connector.”
- “The first production connector.”
- “YScope alone made dynamic loading possible.”

Tim Meehan's announcement describes the contemporary path as enabling out-of-band connectors fully in C++. Treat this as historical implementation context rather than a requirement that CLP remain implemented in C++. It supports the interpretation that:

- Broader plugin infrastructure already existed.
- The YScope PRs completed important missing parts.
- CLP exercised the path as a serious reference implementation.

---

## 8. What Other Presto Native Connector Authors Can Reuse

CLP is one of Presto Native's first independently developed, pluggable connectors and therefore a practical reference implementation for other Presto Native connector authors. This article should explain that lifecycle without reproducing full repository documentation. Keep “one of the first” until the absolute “first” chronology is verified.

### 8.1 Define the contract

A connector needs to answer:

- What catalogs, schemas, and tables exist?
- What columns or query surfaces are exposed?
- How are types represented?
- How are data units represented as splits?
- What connector-specific information must the worker receive?
- Which predicates and projections can be pushed down?
- How does the worker return values to Presto Native?

For CLP, this includes:

- Dataset discovery
- Split-local structure or exposed-field metadata
- Polymorphic type handling
- Archive locations and time bounds
- CLP-specific path/search/projection operations

### 8.2 Divide Java and native responsibilities

Coordinator side:

- Catalog metadata
- Schema/table discovery
- Split planning
- Filter translation
- Serialization of connector-specific handles

Native side:

- Factory registration
- Split/handle deserialization
- Archive access
- Native filter and projection evaluation
- Result-vector construction

The exact CLP source layout and interface names should be verified against the repository before publication.

### 8.3 Build

Educational points:

- Build the coordinator-side component against a compatible Presto version.
- Build the native shared library against compatible Presto Native and Velox interfaces.
- Link or package CLP and other required native dependencies.
- Make version compatibility explicit.
- Run unit tests on each side and an end-to-end test across the boundary.

Do not publish unverified commands. The draft can point readers to the repository's current build instructions.

### 8.4 Package and publish

An out-of-band connector should provide:

- Versioned coordinator artifact
- Versioned native shared library or image
- Catalog configuration template
- Compatibility matrix
- Deployment documentation
- Minimal example dataset
- Smoke-test query
- Release notes for serialization or ABI changes

CLP native packaging, based on the current internal description:

- Builds the connector in a manylinux_2_28 environment to establish a broadly compatible Linux ABI baseline.
- Packages the connector's transitive shared-library dependencies.
- Adjusts RPATH so the dynamic loader resolves libraries from the packaged layout rather than relying on matching system installations.
- This provides portability across compatible Linux distributions; it should not be described as general macOS/Windows cross-platform support.
- Exact build image, RPATH tooling, packaged library set, and release artifact layout should be verified before publication.

The value of out-of-band loading is organizational as well as technical:

- The connector can have its own repository.
- It can be released independently of the Presto monorepo.
- Domain experts can own it.
- Presto remains responsible for stable extension points rather than absorbing every storage engine.

### 8.5 Deploy

Conceptual flow:

1. Install the coordinator-side connector component.
2. Install or mount the native connector library on each worker.
3. Ensure Presto Native is configured to load the plugin.
4. Add the catalog properties.
5. Configure metadata and split providers.
6. Start or restart the cluster.
7. Verify catalog discovery.

The main CLP project provides a minimal end-to-end setup that brings up Presto Native with the connector through Kubernetes or Docker Compose.

### 8.6 Validate

Minimal validation sequence:

- SHOW TABLES
- DESCRIBE a dataset
- Query an exposed common field
- Query a nested/dynamic field
- Exercise a typed polymorphic access
- Confirm metadata pruning
- Confirm connector-side projection/filtering
- Confirm missing-field and NULL behavior
- Confirm coordinator/native compatibility failure is clear

### 8.7 Editorial boundary

Include:

- One compact lifecycle/checklist.
- One warning that coordinator-to-worker handles form a versioned protocol.
- Links to current connector and deployment documentation.

Link out:

- Installation and first-query walkthroughs
- Compiler flags
- Complete dependency installation
- Kubernetes values
- Docker Compose files
- Troubleshooting
- Full configuration matrix

---

## 9. Metadata, Pruning, and Execution

### 9.1 Metadata-provider abstraction

Public documentation exposes interfaces for:

- Metadata provider
- Split provider
- Split-filter provider

Open-source reference implementation:

- The CLP open-source repository includes a basic MySQL-backed implementation for metadata and split discovery.
- It demonstrates the provider interfaces and supplies a usable public path; it is not the more advanced metastore used by Uber or YScope internally.

Extension principle:

- A deployment can implement another provider.
- The connector does not need a hard dependency on one metastore product.

### 9.2 Useful metadata

- Dataset/table identity
- KV-IR or archive URI and format
- Minimum and maximum timestamp
- Record count
- Selected top-level dimensions
- Split-local field/type information
- Lifecycle or retention state
- Optional counts/statistics for supported predicates

The metadata system should index enough to avoid scans, not attempt to become a complete event/token index.

### 9.3 Metadata pruning

Question answered:

> Could this file or archive contain any matching event?

Flow:

1. Extract supported top-level predicates.
2. Translate them into the metadata provider's filter form.
3. Obtain a correctness-preserving candidate set.
4. Generate splits only for those files or archives.
5. Apply residual event-level filtering in CLP or Presto.

Documented supported expression classes include comparisons, logical operators, and certain dereferences, subject to provider configuration.

### 9.4 CLP execution

After pruning, the connector dispatches the reader for each selected format. For an immutable archive split:

- Worker opens or streams an archive.
- CLP uses schema/logtype/statistical information.
- It searches encoded representations.
- It selectively decodes candidate regions.
- It materializes requested fields or matching events.
- Presto continues with joins, aggregation, Top-N, or result delivery.

### 9.5 Object storage

The following describes the immutable archive path in Uber production:

- CLP's archive layout is optimized for object storage: the header, schema tree, dictionaries, and indexes are at the front.
- After initialization, the reader can progress through the archive sequentially with minimal seeking; it does not need backward seeks or repeated jumps around the object.
- The Presto Native worker supplies compute and hosts the CLP connector in-process. The connector opens the object-storage byte stream; the CLP streaming archive reader interprets and searches it.
- No local data-disk staging.
- No archive cache in the described path.
- That CLP archive/file reader begins searching the byte stream as archive data arrives.
- The access pattern avoids paying object-store latency for scattered random reads and avoids downloading a complete archive before execution can start.
- API access is deliberately paced to avoid consuming excessive shared resources.
- Queries already meet user-experience expectations.

The production constraint is therefore governance of a shared object-storage API, not insufficient worker CPU.

Publication wording guardrail: name the front-loaded header, schema tree, dictionaries, and indexes, followed by forward, sequential access with minimal seeking. Do not turn this into a claim that stream setup or object-store range requests have zero cost.

### 9.6 Planning-time early termination: workload specialization used at Uber

This is advanced internal behavior and should be mentioned briefly in the Presto article, not deeply derived. The underlying design materials are not intended for public consumption and must not be linked or cited.

Common query:

~~~sql
SELECT ...
FROM logs
WHERE ...
ORDER BY timestamp DESC
LIMIT 100;
~~~

Properties:

- The query has a finite `LIMIT N` and orders by the log timestamp, normally newest first.
- Each candidate file or archive has trustworthy beginning and ending timestamps.
- File and archive time ranges may overlap.
- Metadata can be streamed in time-aware order.
- Metadata provides exact matching counts or another correctness-preserving bound for supported predicates.
- Presto does not guarantee split execution order and normally runs planned work to completion.
- Eligibility rules must prove that unseen older archives cannot alter the result.
- Internal timestamp ordering and append-only publication can improve efficiency and simplify consistency, but they are helpful properties rather than the core eligibility condition.
- These requirements describe the dominant interactive log-query shape, so most user investigations can benefit even though the optimization is not universal.

Optimization:

1. Apply ordinary metadata pruning.
2. Stream surviving archive metadata in timestamp order.
3. Accumulate sufficient count/bound information.
4. Include archives overlapping the result watermark.
5. Stop metadata consumption when older archives cannot affect global Top-N.
6. Schedule only the proven sufficient split set.

The scheduler remains free to execute selected splits in any order.

Pruning and early termination are orthogonal:

- Pruning eliminates archives that cannot match.
- Early termination eliminates archives that may match but cannot change the ordered limited result.

The performance impact can be substantial: after the result watermark is proven, entire older archive ranges require no additional split generation, object-store reads, or CLP scans. This particularly benefits the dominant interactive pattern of asking for the newest matching events with a small limit. The selected splits still use Presto's normal scheduler; the gain comes from proving that fewer splits need to exist.

---

## 10. Open-Source State

### 10.1 Public connector

- Repository: https://github.com/y-scope/clp-plugin-presto-connector
- Provides the external connector implementation.
- Intended as a usable connector and a reference implementation.

### 10.2 Integration in CLP

- Repository: https://github.com/y-scope/clp
- Guide: https://docs.yscope.com/clp/main/user-docs/guides-using-presto.html
- Users can:
  - Deploy CLP.
  - Compress JSON logs.
  - Enable Presto as the query engine.
  - Expose datasets as tables.
  - Query from the CLP UI or Presto CLI.

Public deployment paths include:

- Kubernetes/Helm
- Docker Compose

### 10.3 Publicly documented connector behavior

- SQL-based read access.
- Searchable KV-IR and immutable archive formats, including mixed queries across both.
- Metadata and split-provider abstraction.
- Basic MySQL-backed reference provider in the CLP open-source project.
- Configurable split filters.
- Polymorphic-type support with type annotations.
- Mapping of CLP primitive types to Presto types.
- Current documentation maps object nodes to Presto ROW.
- Public source also implements dynamic field exposure, schema-less UDF rewriting, native filtering and projection pushdown, typed wildcard value searches, and complete-event JSON projection.
- Read-only; DDL operations are not supported.

### 10.4 Avoid underselling open source

Do not write:

- “The public code only establishes an architecture.”
- “Uber has the real implementation.”
- “The connector is only a preview.”

Preferred:

> The open-source integration provides a usable end-to-end workflow. YScope and Uber worked together to extend and apply the same foundation for Uber-specific infrastructure and scale.

---

## 11. Uber Production Deployment

### 11.1 Uber's role

Best description:

> Principal production design and deployment partner.

Contributions:

- Production use case and requirements.
- Real log data and workload diversity.
- Deployment and operational validation.
- Internal metadata integration.
- Object-storage integration.
- SQL query-routing and observability-platform integration.
- Query planning, pruning, reliability, and operational controls.
- Additional internal optimizations and capabilities built on CLP.

Avoid implying:

- Uber authored the named dynamic-loading PRs.
- Uber engineers are named contributors to those PRs without evidence.

### 11.2 Scale

Approved production figures for the article:

- Approximately 25 PB of raw logs per month for onboarded workloads.
- Approximately 750–850 TB after compression.
- Approximate compression ratio: 29–33×.
- Approximate stored-volume reduction: 97%.
- Five Presto-native workers.
- 48 CPU cores per worker.
- Typical CPU utilization: approximately 5–10%.
- Search directly from internal object storage.
- No local archive cache or data-disk staging.
- Latest log data becomes queryable before files close, generally within approximately 30 seconds to a few minutes.

Important scope:

- 25 PB/month is current onboarded raw volume, not all logs generated across all Uber services.
- Do not claim thousands of services are currently onboarded.
- The μSlope paper's broader Uber log-volume number has a different scope and must not be conflated with this deployment figure.

### 11.3 Resource philosophy

- Goal is not maximum throughput.
- Goal is the lowest practical shared-resource consumption that still keeps the UX responsive.
- Object-storage API rate limiting is deliberate.
- Queries are already fast enough.
- Additional worker CPU would not materially improve the intended user experience under current pacing.

### 11.4 Internal metadata system

User-provided current description:

- Both Uber and YScope maintain more advanced internal metastore implementations than the basic MySQL reference backend shipped with the open-source project.
- The figures below describe Uber's current internal implementation unless otherwise noted.
- Handles metadata for billions of files.
- Supports evolving top-level indexed fields.
- Latest implementation may operate with roughly:
  - 2–4 CPU cores
  - 32 GB RAM
  - About 1 TB disk
- Relational implementation using MySQL/MariaDB.
- Can use ordered, streaming metadata queries.
- Transactions are helpful but not a connector hard requirement.
- Eventual consistency can be sufficient with a defined correctness model.

### 11.5 Public versus internal

Already available in OSS:

- Dynamic field exposure
- Schema-less query rewriting and pushdown
- Polymorphic typed access
- Typed wildcard value searches
- Complete-event projection through `CLP_GET_JSON_STRING()`
- Basic pluggable metadata and split filtering

Not currently available in OSS:

- Planning-time early termination
- Advanced metadata filtering and planning
- Billion-file metadata scale

Uber's deployment also adds Uber-specific:

- Metadata integration
- Split discovery
- Query planning
- Query routing
- Reliability and operational controls
- Other CLP-aware optimizations

Uber and YScope plan to open-source selected generally useful components.

Safe wording:

> Uber and YScope intend to contribute additional reusable components over time; scope and timing remain to be determined.

---

## 12. Collaboration and Attribution

### 12.1 YScope

- Primary contributor to the CLP connector.
- CLP and CLP implementation.
- Connector design and implementation.
- Key upstream changes closing native connector gaps.
- Continued collaboration with Uber on production integration.

Named contributors from Tim Meehan's announcement:

- ChenXing Yang
- Jack Luo
- Others on the two referenced PRs

### 12.2 IBM and Presto community

- Broader Presto-native plugin infrastructure.
- Infrastructure-level development.
- Architectural guidance.
- Upstream review.
- Presto Foundation coordination.

Named:

- Deepak Majeti: substantive review of the referenced work.
- Tim Meehan: Presto chair, community/architectural involvement and public recognition of the contribution.

### 12.3 Uber

- Production design and deployment partner.
- Requirements, scale, data, infrastructure integration, validation, and engineering on top.
- No named Uber contributor to dynamic loading has been established from the current evidence.

### 12.4 Meta

- Feedback and technical discussion.
- Participation through the wider Presto community/Foundation.
- Do not characterize Meta as a primary CLP connector implementer.

### 12.5 Recommended attribution paragraph

> The CLP connector was developed primarily by YScope engineers. Building it also helped complete important infrastructure for out-of-band connectors in Presto Native, with support, infrastructure work, and technical review from IBM engineers and the broader Presto community. YScope engineers then worked alongside Uber engineers to extend and apply the architecture in Uber production. Uber contributed large-scale workloads, platform integration, and operational validation, while both teams collaborated on additional metadata and query-planning engineering.

---

## 13. Future Capabilities

### 13.1 Native Variant and type-agnostic projection

- Presto does not currently expose the first-class Variant type needed to return a planner-stable expression whose runtime value may be a scalar, object, or heterogeneous array.
- If such a type is introduced, CLP can map its own recursive values into Variant for exact-path, type-agnostic projection.
- Existing typed functions should remain:
  - They are convenient for users who know the desired type.
  - They compose directly with typed SQL predicates and aggregations.
  - They provide clear pushdown intent.
- A new Variant-returning accessor is safer than changing existing UDF return types.
- JSON projection remains valuable for clients and workflows that want portable serialized events.

Safe future wording:

> A future Presto Variant type could provide a more natural SQL representation for a polymorphic value at a known path, including nested objects and heterogeneous arrays. CLP's typed accessors would remain useful for strongly typed predicates and pushdown.

### 13.2 Wildcard-field access beyond Variant

- Variant does not discover keys; an exact getter still needs a path.
- Dremio's documented `VARIANT_GET` is a useful comparison: it can preserve or cast the value at an explicit path, but its documented path syntax does not support wildcard paths.
- CLP's split-local schema tree can support a broader future operation:
  - match wildcard or regular-expression field paths;
  - search keys, values, or key-value relationships;
  - return multiple matched paths whose values have different types;
  - preserve nested and heterogeneous values.
- A multi-match result should retain paths as well as values. One possible Variant representation is an array of `{path, value}` objects.
- Filtering and projection should have distinct cardinality semantics:
  - any-match filtering can execute inside CLP without materializing all matches;
  - wildcard projection needs a collection result.
- Keep this future-facing in the article. The current verified public API provides typed wildcard **value searches across fields**, not a general wildcard-field projection function.

Safe future wording:

> Variant would solve type uncertainty at a known path. CLP's structural metadata creates a path toward solving the complementary problem: finding and projecting fields whose names are not known in advance.

### 13.3 Metadata as a query substrate

CLP metadata may include:

- Evolving schema structure
- Observed types
- Logtype/template dictionaries
- Frequencies
- Time bounds
- Archive statistics
- Selected field statistics

Today this supports:

- Discovery
- Split planning
- Pruning
- Encoded search

Potential future uses:

- Dataset summaries
- Dominant-pattern summaries
- New-pattern detection
- Schema-change detection
- Type-change detection
- Anomaly identification
- Search planning
- Semantic-search candidate reduction

### 13.4 Efficient semantic search

Internal direction described by the user:

- Leverage CLP internals rather than indexing every duplicated raw event.
- Operate over deduplicated templates, schema context, and compact metadata.
- Map relevant templates/structures back to source events.
- Target petabyte-scale semantic search with very low cost and CPU-only operation.

Do not publish “for pennies without GPUs” until denominator, workload, latency, resources, and baseline are defined.

Safe wording:

> CLP's compact schema and log-pattern representations also open a path to inexpensive summarization and semantic-search planning. Rather than applying expensive processing to every repeated raw event, future systems can first identify relevant structures and encoded regions.

### 13.5 Open-source direction

- Selected production metadata, planning, and advanced-search components may be open-sourced.
- No promised date, repository, or exact scope.

### 13.6 Local/edge possibility

- CLP archives are independently searchable.
- A future authorized client could potentially stream and search an archive from a laptop or managed edge environment.
- Would require access control, credential handling, API governance, and local resource limits.
- Mention only if the article needs a broader architectural ending.

---

## 14. Editorial Strategy for the Presto Community

### 14.1 Reader profile

Assume familiarity with:

- SQL tables, columns, predicates, ordering, and limits.
- The basic idea that Presto queries external data through catalogs and connectors.

Do not assume familiarity with:

- CLP
- Iceberg Variant or Parquet Variant internals
- Shredding
- ERTs and schema trees
- Logtypes
- Pushdown and splits
- Presto Native plugin loading or coordinator/worker serialization
- Uber's internal logging stack

The article must remain understandable without opening its links. Define each unfamiliar term briefly at first use, then link authoritative documentation or source for readers who want implementation depth.

### 14.2 Reader value

The article should provide:

1. A concrete external native connector case study.
2. A reusable connector lifecycle.
3. A language-design lesson about schema-less data.
4. A storage pushdown lesson.
5. A working open-source project to try.
6. Production credibility.
7. A credible future direction.

### 14.3 Tone

- Technical and direct.
- Causal rather than feature-inventory driven.
- Start with a recognizable problem.
- Explain user-visible behavior before implementation details.
- Tie mechanisms to outcomes.
- Use one architecture diagram.
- Include one SQL example and one execution comparison table.
- Provide practical build/deploy links.
- Avoid excessive Uber organizational history.
- Treat Iceberg Variant as a credible alternative with different physical tradeoffs, not a straw man.
- Label current OSS, Uber production behavior, and future directions at first mention.

### 14.4 Recommended length

- Approximately 1,900–2,300 words.
- Spend most of the budget on the storage distinction and the SQL-to-encoded-column path.
- Keep connector-author guidance compact and link to deployment documentation rather than reproducing it.

### 14.5 Material to omit

- Complete Uber ingestion architecture.
- Pinot hot-tier details.
- Detailed early-termination proof.
- Full metastore schema.
- Exhaustive UDF reference.
- Complete build troubleshooting.
- Full CLP storage-format derivation.
- Unsupported benchmark claims.

---

## 15. Claims Matrix

### Publicly documented

- CLP connector provides SQL access to searchable KV-IR files and immutable CLP archives, including mixed queries across both formats.
- Metadata and split providers are extensible.
- The CLP open-source project includes a basic MySQL-backed reference provider.
- Uber and YScope maintain more advanced internal metastore implementations.
- Configured columns can participate in split filtering.
- Polymorphic types are supported through annotated columns in public docs.
- Public docs map object nodes to Presto ROW.
- Public source provides the verified `CLP_GET_*`, typed `CLP_WILDCARD_*_COLUMN`, and `CLP_GET_JSON_STRING()` functions.
- Public source rewrites dynamic field functions into scan-level requests and pushes compatible predicates toward CLP.
- Connector is read-only.
- Main CLP project offers integrated Presto deployment paths.
- μSlope stores schema structures compactly and searches without full decompression.
- Presto 0.298.1 documents `JSON`, `ROW`, `ARRAY`, and `MAP`, but no first-class `VARIANT` data type.
- Trino 483 documents a first-class `VARIANT` type following the Apache Iceberg Variant specification.
- Trino 483 maps Iceberg `VARIANT` to Trino `VARIANT`; Iceberg Variant requires format-version 3, whose Trino support is currently experimental.
- Trino's documented Variant access uses exact object-key and numeric array subscripts that return `VARIANT`, plus casts to conventional SQL types.
- Parquet Variant shredding is optional and can enable typed projection and data skipping for selected paths; it is a physical-layout capability, not a guarantee of the logical Variant type.
- Parquet Variant recursively represents primitives, arrays of Variant values, and objects containing Variant values.
- Dremio's `VARIANT_GET` can return Variant or a requested type from an explicit path; its documented path syntax does not support wildcards.
- PR #27653 concerns dynamic connector loading/registration.
- PR #27652 concerns connector-specific serialized-value routing.

### Approved Uber production facts

- 25 PB/month raw onboarded logs.
- 750–850 TB/month compressed.
- Five 48-core workers.
- 5–10% typical CPU.
- Direct internal object-storage search with no local archive cache/disk staging.
- Object-storage API rate limiting is deliberate shared-resource governance.
- 30-second-to-few-minute freshness is sufficient.
- Billions of file records in internal metadata.
- Approximate internal metastore footprint.
- Advanced pruning, planning, and early termination.
- Planned future open-sourcing of selected components.

### Requires verification before publication

- Exact two-sided wildcard/grep and wildcard-key behavior for the release discussed in the article.
- Exact Parquet comparison corpus, codec/settings, sizes, and multiplier.
- Exact current build/package commands.
- Exact artifact publication mechanism.
- Exact Presto/Velox compatibility story.
- “First” versus “one of the first” native external connector wording.
- Named Uber contributors, if any, to upstream plugin infrastructure.
- Any partner-scale statement.
- Any semantic-search cost/performance claim.

---

## 16. Open Questions

### Query interface

- Which function names are deployed at Uber?
- Which function names are publicly released?
- Can wildcard expressions match keys, values, or both in the current public release?
- How is any-type polymorphic access exposed?
- Should a future type-agnostic exact-path accessor return Presto `JSON`, serialized JSON, or native `VARIANT` when available?
- What missing-path versus present-null semantics should a future type-agnostic accessor expose?
- Should a future wildcard projection return only values, a path-to-value object, or an ordered collection of `{path, value}` matches?
- What should typed wildcard projection do with incompatible value types: fail, return `NULL`, or select only matching types?
- Which wildcard grammar should be used for one path component, recursive descent, array elements, and literal special characters?
- How is a complete event represented?
- How are special-character and case-sensitive paths escaped?

### Architecture

- Which connector responsibilities live in the Java coordinator versus native workers?
- Where exactly does expression rewriting occur?
- What concrete objects use customSerializedValue?
- What is the exact native plugin entry point and factory-registration API?

### Build and release

- What artifact is installed on the coordinator?
- What shared library is installed on workers?
- Is there a published compatibility matrix?
- Are release binaries published, or is source/image build the current public path?
- What is the smallest supported end-to-end setup?

### Production

- Publishable example of pruning and/or early termination.
- Whether to mention internal metastore resources.
- Whether to mention freshness.
- Whether to mention partner deployments.

### Attribution

- Final author list.
- Whether Tim Meehan should be named in prose or acknowledgments.
- Whether Deepak Majeti should be named in prose or acknowledgments.
- Whether individual YScope contributors should be named in the main section.

---

## 17. Reference Index

### CLP and research

- CLP: https://github.com/y-scope/clp
- CLP Presto connector: https://github.com/y-scope/clp-plugin-presto-connector
- μSlope, OSDI '24: https://www.usenix.org/conference/osdi24/presentation/wang-rui
- Earlier text-log CLP paper, OSDI '21: https://www.usenix.org/conference/osdi21/presentation/rodrigues

### Documentation and design

- CLP connector docs: https://docs.yscope.com/presto/connector/clp.html
- Using Presto with CLP: https://docs.yscope.com/clp/main/user-docs/guides-using-presto.html
- CLP schema, ERT, column, and log-text encoding design: https://docs.yscope.com/clp/main/dev-docs/design-kv-ir-streams/background.html
- CLP connector RFC: https://github.com/y-scope/presto-rfcs/blob/7ca6b8af915d5f35c5a5ce257e8c548edb0b3c00/RFC-0012-clp-connector.md
- Internal early-termination design material exists, but it is not a public publication reference and must not be linked or cited.
- Presto data types (0.298.1): https://prestodb.io/docs/current/language/types.html
- Presto JSON functions (0.298.1): https://prestodb.io/docs/current/functions/json.html
- Trino 483 Variant type and functions: https://trino.io/docs/current/functions/variant.html
- Trino 483 Iceberg connector and Variant mapping: https://trino.io/docs/current/connector/iceberg.html
- Trino 481 release note introducing experimental Iceberg Variant support: https://trino.io/docs/current/release/release-481.html
- Apache Iceberg v3 Variant specification: https://iceberg.apache.org/spec/#semi-structured-types
- Parquet Variant encoding: https://parquet.apache.org/docs/file-format/types/variantencoding/
- Parquet Variant shredding: https://parquet.apache.org/docs/file-format/types/variantshredding/
- Dremio `VARIANT_GET`: https://docs.dremio.com/dremio-cloud/sql/sql-functions/functions/VARIANT_GET/

### Presto-native infrastructure

- PR #27653: https://github.com/prestodb/presto/pull/27653
- PR #27652: https://github.com/prestodb/presto/pull/27652

### Relevant Presto blog references

- Presto blog index: https://prestodb.io/blog/
- Presto–Lance connector: https://prestodb.io/blog/2026/06/21/presto-lancedb-connector-querying-vector-embeddings-with-distributed-sql/
- Connector federation with Arrow Flight: https://prestodb.io/blog/2026/06/15/bridging-java-and-native-execution-presto-connector-federation-with-apache-arrow-flight/

---

## 18. Working Abstract

CLP is an open-source system for losslessly compressing and directly searching data whose structures and types evolve over time. Instead of storing each event as one dynamic row value or forcing every observed field into a global sparse table, CLP groups recurring key-and-type schemas into encoded record tables and columnizes every schema node. It also separates recurring text patterns from typed variables inside string values. The CLP connector carries SQL path and predicate intent through Presto planning and pushdown, then binds it to those encoded columns inside each Presto Native worker. The open-source connector queries searchable KV-IR files and immutable archives—including mixed queries across both—and provides typed exact-path access, wildcard value search, complete-event JSON, and pluggable metadata filtering today. The ERT, schema-tree, and sequential object-storage walkthrough describes the immutable archive path. At Uber, the same architecture serves workloads producing approximately 25 PB of raw logs per month directly from object storage. Future PrestoDB Variant support could expose one logical `event VARIANT` without changing CLP storage, while schema-tree-backed key discovery remains a complementary future direction. The result is both a columnar-by-design approach to schema-less analytics and a reference path for independently developed Presto Native connectors.
