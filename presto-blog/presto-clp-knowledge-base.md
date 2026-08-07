# Presto–CLP Connector: Knowledge Base

**Status:** Working source of truth for a Presto community article  
**Prepared:** August 6, 2026; updated August 7, 2026  
**Purpose:** Preserve the full technical, editorial, attribution, deployment, and future-looking context before distilling it into a publication outline and draft.

> This document is intentionally more detailed than the eventual article. It distinguishes publicly documented behavior, Uber production extensions, informed architectural interpretation, and facts that still require verification.

---

## 1. Executive Summary

- CLP, the Compressed Log Processor, is an open-source system for losslessly compressing and searching logs.
- The connector and query model are not limited to log analysis. They apply to schema-less and semi-structured data more broadly; logs are a particularly strong production use case because they combine repeated patterns, evolving fields, polymorphism, and high volume.
- It supports the spectrum from natural-language messages with embedded variables to full JSON events with evolving structures and polymorphic field types. “Logs” includes both traditional service/operations records and newer execution traces such as coding-agent runs with tool calls, structured state, and natural-language output.
- CLP-S is the schema-aware, semi-structured part of the system. It represents repeated JSON structure compactly within each archive/split, maintains the structural metadata needed to interpret that split, and searches encoded data without first reconstructing every event as raw JSON.
- CLP-S does not require one global schema tree spanning every split:
  - Different splits may contain different fields, nesting, and observed types.
  - A new split can introduce structure without mutating a dataset-wide schema contract.
  - Schema-less functions use each split's own structural information at execution time.
  - A deployment may optionally derive a global view of common fields for discovery and conventional SQL columns; that view can coexist with pure schema-less UDF access.
- CLP-S is implemented in C++, making it a natural fit for Presto Native:
  - The reader and search engine can be embedded directly in a native worker.
  - The connector can be loaded as a shared library.
  - The scan path needs no sidecar, JNI boundary, or additional RPC hop.
- The CLP connector is also important to Presto itself:
  - It is an early real-world reference implementation of an independently developed, out-of-band Presto-native C++ connector.
  - Building it exposed missing infrastructure for dynamic connector registration and connector-specific coordinator-to-worker serialization.
- The connector combines two access paths:
  - **Schema-aware access:** an optional global view can expose common automatically discovered fields through familiar SQL columns.
  - **Schema-less access:** functions can search and project paths, values, types, or complete events that are not represented in the planned table schema.
- These access paths coexist. A query can narrow data using ordinary timestamp, service, or severity columns, then use CLP functions to explore dynamic fields.
- Schemas and types evolve in producer applications, often without coordination with or visibility to the Presto operators responsible for the query infrastructure.
- Presto 0.298.1 has a `JSON` type but no first-class `VARIANT` type:
  - A conventional expression still needs one result type during planning.
  - `ROW` has a fixed planned shape and `ARRAY(T)` has one planned element type.
  - CLP-S is more permissive: object values and array elements may be nested and heterogeneous, and the same path may have different types across events.
- The current connector bridges this gap with planner-typed accessors and complete-event JSON projection. A future Presto `VARIANT` type could provide a lossless type-agnostic result for a known path without replacing the existing typed functions.
- `VARIANT` would not, by itself, solve unknown-key access. Exact-path retrieval and wildcard field discovery are separate problems; CLP's split-local schema tree creates a path toward efficient wildcard-key projection and search.
- The functions close a language expressiveness gap. Standard relational SQL cannot naturally search every value without enumerating columns, address a path absent from the planned table, or project a field unknown during planning.
- The functions are not intended to behave like record-at-a-time JSON parsing:
  - They carry schema-less intent through planning.
  - Supported expressions are rewritten or translated into connector operations.
  - Filtering and projection are pushed into CLP-S.
- Query efficiency has two layers:
  1. **Work avoidance:** metadata pruning and, in Uber production, planning-time early termination reduce archives and splits.
  2. **Fast remaining execution:** CLP-S searches the compressed representation and selectively reconstructs matching regions.
- The open-source integration is usable end to end today and is integrated into the main CLP repository.
- YScope and Uber worked together to extend and apply the architecture in Uber's production environment, including integration with Uber's metadata, object-storage, query-routing, and observability infrastructure.
- Uber is best described as the principal production design and deployment partner:
  - Production requirements and real workloads
  - Deployment and operational validation
  - Metadata and object-storage integration
  - Query planning, routing, pruning, reliability, and other production engineering
- YScope engineers are the primary connector contributors.
- IBM engineers and the Presto community contributed surrounding native infrastructure, architectural support, and review.
- The article should teach both:
  - How CLP makes schema-less data analytics possible through Presto, using logs as the principal production case study.
  - How another team can structure, build, package, deploy, and maintain an external native connector.

### Central thesis

> CLP demonstrates two extensions to Presto's reach: a practical path for independently developed native connectors, and a way to make schema-less operations expressible in SQL without giving up storage-native performance.

### Supporting formulations

- The schema is a convenience and an optimization, not a boundary around the data.
- Common fields provide familiar SQL; schema-less functions unlock the long tail.
- The UDF is the SQL surface, query rewriting carries the intent, and CLP-S performs the execution.
- Metadata avoids work; CLP-S makes the remaining work inexpensive.
- CLP is not merely another file reader. It brings a specialized C++ storage and search engine into Presto Native.
- Compression does more than reduce bytes: it creates compact structure that can guide queries.

---

## 2. Terminology

### CLP

- Compressed Log Processor.
- Open-source system and surrounding deployment components for ingesting, compressing, searching, and viewing logs.
- Main repository: https://github.com/y-scope/clp

### CLP-S

- Working name in this article for CLP's semi-structured/schema-aware storage and search engine.
- Handles JSON and other semi-structured events whose field sets, nesting, and types may vary.
- Implemented in C++.
- Uses structural representations, typed value encodings, logtype information, and archive metadata.
- Searches its encoded representation rather than requiring a full raw JSON reconstruction before evaluation.

### μSlope

- Research system/paper underlying important CLP-S techniques:
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

- CLP-S arrays do not require every element to have the same type.
- An array element may itself be an object or another heterogeneous array.
- Object values may likewise be scalars, arrays, or objects with independently evolving structure.
- This cannot be represented faithfully as a conventional Presto `ARRAY(T)` unless `T` is itself a type-erasing envelope such as `JSON` or a future `VARIANT`.

### Variant

- Presto 0.298.1 does not currently document a first-class `VARIANT` data type comparable to Parquet Variant or Dremio Variant.
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
- Possible operations include:
  - Dynamic path projection
  - Typed projection
  - Key-only search
  - Value-only search
  - Dynamic key-value search
  - Wildcard or regular-expression key matching
  - Search across types
  - Complete-event reconstruction
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
Request <*> failed after <*> ms
~~~

The variables are stored separately using appropriate encodings.

For JSON, keys, paths, nesting, and structural patterns also repeat within an archive. CLP-S can:

- Represent recurring structure compactly.
- Avoid repeating every key in every event.
- Separate structure from values.
- Preserve observed types.
- Group or organize events with related structures.
- Retain sufficient metadata to navigate and search that split's encoded data.

### 3.3 Structural scope is per split

- CLP-S does not need a single global schema tree covering the dataset.
- Each split carries or references the structure needed to interpret its own events.
- Different splits can contain different fields, nesting, and type variants.
- Within an event, objects and arrays may also contain recursively nested values of different types; CLP-S does not impose SQL's homogeneous `ARRAY(T)` model on the stored data.
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

### 3.5 Why C++ matters

- CLP-S's performance-critical reader, decoder, and search implementation are C++.
- Presto Native executes its worker data path in C++.
- A native connector can link directly to CLP-S.
- Direct integration avoids:
  - A separate CLP query service in the scan path
  - Additional RPC transport
  - Per-record JNI calls
  - Reimplementation of the search engine in Java
  - Loss of CLP-S's encoded execution model

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
| Parquet or ORC file | CLP-S archive |
| File or row-group split | CLP archive split |
| Parquet or ORC reader | Native CLP-S search and projection engine |
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

- CLP-S is not merely a generic columnar reader.
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

JSON functions can approximate some tasks, but they are database-specific extensions and typically operate on an opaque document value. They do not by themselves preserve CLP-S's compressed execution.

JSON remains a useful compatibility representation at the query boundary. The distinction is between storing and repeatedly parsing every event as an opaque JSON document versus asking CLP-S to evaluate an operation against its encoded representation and serialize only the selected value or matching complete event.

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

### 5.6 Planning and pushdown

Intended mechanism:

1. Presto parses and type-checks the SQL expression.
2. The connector recognizes supported CLP functions and predicates.
3. It translates them into connector-specific handles or operations.
4. Connector-specific information reaches the native worker.
5. CLP-S evaluates compatible filtering and projection against the encoded archive.
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

### 5.8 Current type-agnostic access and a future Variant path

Current implemented surface:

- Typed accessors such as `CLP_GET_BIGINT(path)` give Presto a stable planning-time type.
- Different typed accessors can address different observed types at the same path.
- `CLP_GET_JSON_STRING()` is the currently verified type-agnostic projection for the **complete event**.
- Do not claim that the public connector currently provides `CLP_GET_JSON_STRING(path)` unless that overload is implemented and verified.
- Serialized JSON can faithfully represent JSON strings, numbers, booleans, nulls, nested objects, and heterogeneous arrays, but it does not preserve richer non-JSON type tags without an explicit serialization convention.

If Presto adds a native Variant type:

- Add a new type-agnostic accessor rather than changing an existing UDF's return type and breaking queries.
- An illustrative API could be `CLP_GET(path) -> VARIANT`, while the existing typed accessors remain useful for typed predicates, aggregation, and pushdown.
- The returned Variant could preserve a scalar, object, or recursively heterogeneous array at the requested path.
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

- Dynamically loads the external CLP connector shared library.
- Receives plan fragments, splits, and connector-specific serialized values.
- Invokes CLP-S directly.
- Produces vectors/pages for downstream Presto operators.

#### Metadata provider

- Provides table metadata, selected common fields, and split-local structure metadata.
- Discovers archives.
- Produces or filters splits.
- May expose time ranges, selected dimensions, record counts, and other statistics.
- Interfaces allow implementations beyond the default open-source provider.

#### CLP-S archives

- Stored on a filesystem or object storage, depending on deployment.
- Uber production streams directly from internal object storage.
- No local archive cache or local-disk staging is used in the described Uber query path.

### 6.2 Proposed high-level diagram

~~~mermaid
flowchart TB
    user["SQL clients"]

    subgraph presto["Presto"]
        coordinator["Java coordinator<br/>Parse · analyze · plan · schedule"]
        coordPlugin["CLP coordinator integration<br/>Metadata · split planning"]
        worker["Presto-native workers"]
        nativePlugin["Native CLP connector loaded as a shared library<br/>CLP-S search · filter · project"]
    end

    subgraph storage["CLP storage"]
        metadata["Pluggable metadata provider<br/>Split structure · archives · statistics"]
        archives["CLP-S archives<br/>Filesystem or object storage"]
    end

    user --> coordinator
    coordinator --> coordPlugin
    coordPlugin <--> metadata
    coordPlugin --> worker
    worker --> nativePlugin
    nativePlugin --> archives
    nativePlugin --> worker
    worker --> coordinator
    coordinator --> user
~~~

### 6.3 Why shared-library integration matters

- Directly embeds an existing C++ storage engine.
- Preserves CLP-S's native algorithms.
- Avoids a remote service boundary in the hot scan path.
- Lets the connector remain independently developed.
- Enables separate release and packaging.
- Requires careful compatibility management with Presto Native and Velox.

---

## 7. Enabling External Native Connectors

### 7.1 The infrastructure gap

Before this work, independently developed fully C++ connectors were not yet a mature real-world Presto-native path.

Required capabilities included:

- Load a connector shared library outside the core binary.
- Register its connector factory.
- Avoid requiring connector source to be compiled into Presto.
- Route connector-specific serialized data from the Java coordinator.
- Deserialize custom handles on the native worker.
- Maintain a compatible contract across Java and C++.

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
- “One of Presto Native's first real out-of-band C++ connectors.”
- “Helped complete important missing infrastructure for external native connectors.”

Avoid until chronology is verified:

- “The first native connector.”
- “The first production connector.”
- “YScope alone made dynamic loading possible.”

Tim Meehan's announcement says the PRs “fill an important gap” and enable out-of-band connectors fully in C++. This supports the interpretation that:

- Broader plugin infrastructure already existed.
- The YScope PRs completed important missing parts.
- CLP exercised the path as a serious reference implementation.

---

## 8. Building, Packaging, Deploying, and Publishing a Connector

This article should serve an educational purpose for future connector authors. It should explain the lifecycle without reproducing full repository documentation.

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
- Link or package CLP-S and other required native dependencies.
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

- One small component diagram.
- One compact lifecycle/checklist.
- One minimal “from deployment to first query” path.

Link out:

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
- Archive URI
- Minimum and maximum timestamp
- Record count
- Selected top-level dimensions
- Split-local field/type information
- Lifecycle or retention state
- Optional counts/statistics for supported predicates

The metadata system should index enough to avoid scans, not attempt to become a complete event/token index.

### 9.3 Metadata pruning

Question answered:

> Could this archive contain any matching event?

Flow:

1. Extract supported top-level predicates.
2. Translate them into the metadata provider's filter form.
3. Obtain a correctness-preserving candidate set.
4. Generate splits only for those archives.
5. Apply residual event-level filtering in CLP-S or Presto.

Documented supported expression classes include comparisons, logical operators, and certain dereferences, subject to provider configuration.

### 9.4 CLP-S execution

After pruning:

- Worker opens or streams an archive.
- CLP-S uses schema/logtype/statistical information.
- It searches encoded representations.
- It selectively decodes candidate regions.
- It materializes requested fields or matching events.
- Presto continues with joins, aggregation, Top-N, or result delivery.

### 9.5 Object storage

Uber production:

- Direct object-storage reads.
- No local data-disk staging.
- No archive cache in the described path.
- Search begins as bytes arrive.
- API access is deliberately paced to avoid consuming excessive shared resources.
- Queries already meet user-experience expectations.

The production constraint is therefore governance of a shared object-storage API, not insufficient worker CPU.

### 9.6 Planning-time early termination: Uber extension

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

- Logs are append-only.
- The query has a finite `LIMIT N` and orders by the log timestamp, normally newest first.
- Each archive is internally ordered by timestamp.
- Archive time ranges may overlap.
- Metadata can be streamed in time-aware order.
- Presto does not guarantee split execution order and normally runs planned work to completion.
- Eligibility rules must prove that unseen older archives cannot alter the result.
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
- Additional internal optimizations and capabilities built on CLP-S.

Avoid implying:

- Uber authored the named dynamic-loading PRs.
- Uber engineers are named contributors to those PRs without evidence.

### 11.2 Scale

User-provided internal figures:

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
- Other CLP-S-aware optimizations

Uber and YScope plan to open-source selected generally useful components.

Safe wording:

> Uber and YScope intend to contribute additional reusable components over time; scope and timing remain to be determined.

---

## 12. Collaboration and Attribution

### 12.1 YScope

- Primary contributor to the CLP connector.
- CLP and CLP-S implementation.
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
  - any-match filtering can execute inside CLP-S without materializing all matches;
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

- Leverage CLP-S internals rather than indexing every duplicated raw event.
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

- CLP-S archives are independently searchable.
- A future authorized client could potentially stream and search an archive from a laptop or managed edge environment.
- Would require access control, credential handling, API governance, and local resource limits.
- Mention only if the article needs a broader architectural ending.

---

## 14. Editorial Strategy for the Presto Community

### 14.1 Reader profile

Assume familiarity with:

- Catalogs
- Connectors
- Hive
- Iceberg
- Metastores/catalogs
- Splits
- Predicate/projection pushdown
- Java coordinator and native workers at a high level

Do not assume familiarity with:

- CLP
- CLP-S
- Logtypes
- Split-local schema/structure trees
- Encoded log search
- Uber's internal logging stack

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
- Feature/capability oriented.
- Start with a recognizable problem.
- Explain user-visible behavior before class-level internals.
- Tie mechanisms to outcomes.
- Use one architecture diagram.
- Include two SQL examples.
- Provide practical build/deploy links.
- Avoid excessive Uber organizational history.

### 14.4 Recommended length

- Approximately 2,700–3,100 words.
- Longer than a focused feature announcement because it also teaches connector development.
- Still shorter and narrower than the Uber Engineering CLP Part III draft.

### 14.5 Material to omit

- Complete Uber ingestion architecture.
- Pinot hot-tier details.
- Detailed early-termination proof.
- Full metastore schema.
- Exhaustive UDF reference.
- Complete build troubleshooting.
- Full CLP-S storage-format derivation.
- Unsupported benchmark claims.

---

## 15. Claims Matrix

### Publicly documented

- CLP connector provides SQL access to CLP archives.
- Metadata and split providers are extensible.
- The CLP open-source project includes a basic MySQL-backed reference provider.
- Uber and YScope maintain more advanced internal metastore implementations.
- Configured columns can participate in split filtering.
- Polymorphic types are supported through annotated columns in public docs.
- Public docs map object nodes to Presto ROW.
- Public source provides the verified `CLP_GET_*`, typed `CLP_WILDCARD_*_COLUMN`, and `CLP_GET_JSON_STRING()` functions.
- Public source rewrites dynamic field functions into scan-level requests and pushes compatible predicates toward CLP-S.
- Connector is read-only.
- Main CLP project offers integrated Presto deployment paths.
- μSlope stores schema structures compactly and searches without full decompression.
- Presto 0.298.1 documents `JSON`, `ROW`, `ARRAY`, and `MAP`, but no first-class `VARIANT` data type.
- Parquet Variant recursively represents primitives, arrays of Variant values, and objects containing Variant values.
- Dremio's `VARIANT_GET` can return Variant or a requested type from an explicit path; its documented path syntax does not support wildcards.
- PR #27653 concerns dynamic connector loading/registration.
- PR #27652 concerns connector-specific serialized-value routing.

### User-provided Uber production facts

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
- Production measurement windows.
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

- Which connector responsibilities live in Java versus C++?
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

- Approved scope/window for scale and utilization figures.
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
- CLP connector RFC: https://github.com/y-scope/presto-rfcs/blob/7ca6b8af915d5f35c5a5ce257e8c548edb0b3c00/RFC-0012-clp-connector.md
- Internal early-termination design material exists, but it is not a public publication reference and must not be linked or cited.
- Presto data types (0.298.1): https://prestodb.io/docs/current/language/types.html
- Presto JSON functions (0.298.1): https://prestodb.io/docs/current/functions/json.html
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

CLP is an open-source C++ system for losslessly compressing and directly searching data whose structures and types evolve over time. Its recursive data model can preserve polymorphic object fields and heterogeneous nested arrays even though Presto currently has no first-class Variant type. Logs are a particularly strong use case, but the connector model applies to schema-less and semi-structured data more broadly. Integrating CLP with Presto required more than implementing another file reader: it required completing an external connector path for Presto Native and extending the SQL query surface so schema-aware columns and schema-less path, value, type, and projection operations could coexist. The open-source connector uses pluggable metadata and split providers to discover datasets and avoid irrelevant archives, then loads CLP-S directly into native workers as a shared library so pushed-down operations execute against the compressed representation. YScope engineers were the primary connector contributors; IBM engineers and the broader Presto community supported surrounding infrastructure and review; and YScope worked alongside Uber engineers to extend and apply the architecture with Uber's internal metadata, object-storage, routing, and planning capabilities. A future Presto Variant type could improve type-agnostic exact-path projection, while CLP's schema tree could additionally support wildcard-field discovery and projection. Beyond log search, CLP's compact schema and pattern metadata may provide a foundation for inexpensive summarization and semantic-search planning.
