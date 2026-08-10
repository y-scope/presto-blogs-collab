# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

Presto gives storage systems with different physical strengths a common SQL surface. For semi-structured data, however, a common value type does not determine how records are organized, compressed, or searched.

Analytical platforms rarely control the applications producing logs, traces, audit records, or AI coding-agent sessions. Fields, nesting, and types change independently. JSON preserves that long tail but gives up field-level columnar execution; a global schema requires coordination the platform rarely has.

[CLP from YScope](https://github.com/y-scope/clp) is a native columnar format and search engine for semi-structured data; its pluggable Presto Native connector is this article's focus. YScope's August 2025 post, [“Querying CLP-compressed logs with SQL via Presto”](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82), explains the storage model, SQL access, pushdown, and original integration. That implementation executed CLP natively in the Velox-based workers of a Presto fork, but required changes in the fork and Velox.

Variant provides the lakehouse reference: it spans SQL, table, and file layers, although PrestoDB does not yet support it end to end. CLP is available through an out-of-band Presto Native connector loaded as a shared library. The contrast explains both the specialized storage behavior that connector preserves and the reusable extension boundary it establishes.

## Variant in the Lakehouse Landscape

Semi-structured storage has historically forced a choice between coordinated typed columns and flexible document values. Variant defines a middle contract: one dynamic value whose nested fields retain their types and can move between supporting engines and lakehouse tables. That standardizes representation; physical acceleration remains a separate decision.

A usable Variant stack spans several layers. The engine supplies a dynamic SQL type and its operators; [Iceberg v3](https://iceberg.apache.org/spec/#variant-type) places that value in an interoperable table; Parquet defines its [binary encoding](https://parquet.apache.org/docs/file-format/types/variantencoding/) and optional [shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/). The Parquet project's [overview](https://parquet.apache.org/blog/2026/02/27/variant-type-in-apache-parquet-for-semi-structured-data/) and Databricks' [introduction](https://www.databricks.com/blog/introducing-variant-new-open-standard-semi-structured-data-apache-parquettm-delta-lake) provide background.

[Trino's `VARIANT`](https://trino.io/docs/current/functions/variant.html)—in the query engine forked from Presto—shows those layers connected through its [Iceberg connector](https://trino.io/docs/current/connector/iceberg.html). It is useful precedent, not a drop-in PrestoDB alternative. Our comparison begins below that future SQL surface: which storage and scan behavior should a connector preserve?

Unshredded Variant is a Parquet binary column of row-local dynamic values. Its `metadata` and `value` representation preserves types and lets a reader navigate a known path without parsing text, but inner fields do not have independent Parquet column encodings or column statistics. Shredding promotes writer-selected paths such as `request.id` into typed Parquet columns, enabling field-level compression, projection, statistics, and data skipping while `value` preserves everything else.

It fits workloads prioritizing portability and standard table semantics when writers can identify hot paths before finalizing each file's physical schema. Shredded paths are encoded in that schema, so adding one after the file is written requires rewriting it. One `typed_value` column represents one declared type; values of other types remain valid in `value`. Promoting every observed path and type would instead create a wide, sparse physical union with additional schema, column, and metadata overhead.

## What the Connector Preserves: CLP's Native Columnar Model

YScope's [August 2025 post](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) gives the fuller introduction to ERTs, archive-local schema trees, logtypes, and their original Presto integration. This section intentionally restates only the storage concepts needed to compare CLP with Variant.

Physically, Parquet Variant and CLP begin in opposite places. Parquet Variant is value-first: each row contains a native dynamic value, and optional shredding adds typed columns for selected paths. CLP is column-first. An immutable archive groups records with the same key-and-type shape into **encoded record tables (ERTs)**. An archive-local schema tree records those shapes, and every observed schema node receives an encoded column. When a logical path changes type, it can map to separate type-homogeneous columns. The result combines field locality with recurring-shape locality without a promotion list or Variant remainder.

That schema tree is execution metadata, not merely an ingestion record. Iceberg metadata and Parquet statistics can prune snapshots, files, row groups, or pages when statistics exist; the Variant standard does not define an archive-local inventory mapping every observed path and type to a physical column. The CLP connector uses that map to resolve exact paths and typed wildcard searches once per archive split rather than rediscovering each event's structure. An optional **convenience schema** can expose common path-and-type bindings after ingestion without rewriting archives.

CLP also goes below the string boundary, separating recurring free-form text into **logtypes** and typed variables. Free-text search can match encoded patterns and variables without reconstructing every source string. Shredding a Variant path as a string leaves its contents as one string unless another index parses it. Complete path/type mapping, cross-field execution, and logtype-aware search are therefore CLP storage properties. Together with homogeneous typed streams, they can improve compression materially on repetitive logs while keeping the data directly searchable.

Those advantages remain workload-dependent. CLP is a strong fit when the priorities are native columnar storage for the full semi-structured long tail, cross-field and free-text search, log-specific compression, and lower object-storage transfer and request costs from compact archives. Variant with selective shredding is a stronger fit when interoperable lakehouse tables and predictable hot paths matter more. With one stable schema, both approaches columnize the same fields; with nearly unique event shapes, CLP coalesces small ERTs, but its shape locality fades. YScope's earlier benchmarks used conventional Parquet, so they do not establish an ordering against Variant or Variant shredding.

## How Predicates Bind to Different Storage

The [August 2025 YScope post](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) already traces typed CLP accessors through UDF rewriting, split discovery, pushdown, and native execution. For the pluggable connector, the important question begins after Presto pushes down a predicate: which physical data must the reader touch?

| Predicate | Iceberg/Parquet Variant | CLP |
|---|---|---|
| Exact named path | Use a shredded column if selected; otherwise traverse `value` | Resolve the path and type once per archive split, then scan matching ERT columns |
| Polymorphic path | Use `typed_value` for its declared type; other types remain in `value` | Route each type to its own homogeneous ERT columns |
| Cross-field typed value search | Inspect each row's relevant `value` structure unless another index covers the search | Resolve candidate paths from archive schema metadata, then scan the corresponding encoded columns |

Exact paths can be efficient in either format when Variant has shredded them. Otherwise, polymorphic or cross-field predicates inspect relevant `value` pages unless an auxiliary index covers the search. That means scanning the Variant column and each selected row's binary structure, not unrelated columns or textual JSON. CLP instead resolves candidate paths and types once per archive split, then scans the encoded columns. Typed cross-field value predicates are current; wildcard key discovery and general `{path, value}` projection remain future work.

Future PrestoDB Variant support could expose one `event VARIANT` over either storage model without making their scans identical. Constant paths could follow CLP's existing resolution and pushdown route, while selecting a complete event would still require reconstruction. A standardized SQL value and a specialized physical reader are complementary parts of the connector contract.

## Making Specialized Storage Pluggable Without a Presto Fork

Making CLP pluggable should preserve its storage-native scan without requiring a Presto fork. [Presto Native, documented under Presto C++](https://prestodb.io/docs/current/presto-cpp.html), pairs the Java coordinator with native workers. The earlier integration already executed CLP in the Velox workers of a Presto fork; it was not an external plugin. The current connector moves that integration out of band while keeping execution native: each worker loads its component as a shared library.

In [Presto's data-source model](https://prestodb.io/docs/current/overview/concepts.html), the connector adapts CLP to catalogs, tables, and splits. One query may span searchable KV-IR files and immutable archives. Coordinator metadata filters objects and creates splits; archive-local metadata maps requested paths and types to ERT columns inside the worker, avoiding a catalog-wide union of the long tail.

~~~mermaid
flowchart TB
    client["SQL client"]
    coordinator["Java coordinator"]
    javaPlugin["CLP coordinator plugin<br/>Metadata · splits · handle serialization"]
    workers["Presto Native workers"]
    loader["Native plugin loader"]
    plugin["Out-of-band CLP connector .so<br/>Handle deserialization · scan"]
    storage["CLP storage<br/>Searchable KV-IR · immutable archives"]

    client --> coordinator
    coordinator <--> javaPlugin
    coordinator --> workers
    workers --> loader
    loader --> plugin
    plugin --> storage
    plugin --> workers
    workers --> coordinator
    coordinator --> client
~~~

The Java side handles catalog exposure, discovery, split planning, expression rewriting, and connector-handle serialization. Native workers register the connector, deserialize those handles, invoke CLP, and return columnar batches. This division lets Presto plan a common SQL query while worker-side code preserves CLP's physical behavior. The in-process shared-library boundary avoids sidecar RPC, per-record language bridges, and raw-event materialization before search without tying CLP permanently to one implementation language.

Building the connector exposed two missing hooks. [PR #27653](https://github.com/prestodb/presto/pull/27653) enabled connector-factory registration from dynamically loaded code; [PR #27652](https://github.com/prestodb/presto/pull/27652) routed connector-specific serialized values to native deserialization. An external connector needs both: the worker must discover its factory and decode the handles planned by Java.

The architectural change is the extension boundary: native execution moved out of the fork, not out of the worker.

## Using the Connector

For an operator, the two-part architecture becomes a short path: ingest with CLP, install the matched Java coordinator plugin and native worker bundle, enable the worker loader, and create `etc/catalog/clp.properties`. The [production configuration](https://docs.yscope.com/presto/connector/clp.html) reads the MySQL metadata populated by the YScope CLP OSS package; custom providers can replace that source. The connector is read-only and exposes discovered datasets in its `default` schema. Its [packaging flow](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/tools/build-packages/README.md) supports OS packages, tarballs, and an init-container image.

A query can combine exact typed paths, cross-field search, and complete-event reconstruction:

~~~sql
SHOW TABLES FROM clp.default;

SELECT
    CLP_GET_STRING('service.name') AS service,
    CLP_GET_BIGINT('response.status') AS status,
    CLP_GET_JSON_STRING() AS event
FROM clp.default.application_logs
WHERE CLP_GET_BIGINT('response.status') >= 500
  AND CLP_WILDCARD_STRING_COLUMN() LIKE 'timeout%'
LIMIT 100;
~~~

Replace `application_logs` and the paths with a deployed dataset. Typed accessors state the expected runtime type; the wildcard predicate searches unnamed string fields; and full JSON is reconstructed only for projected matches. YScope's [August 2025 walkthrough](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) covers rewriting and pushdown.

## Metadata Before and Within Each Split

That workflow hides two metadata scales. The connector's provider filters KV-IR files and archives, then streams candidate splits to the coordinator. At Uber, this layer tracks location, format, KV-IR/archive precedence, time bounds, selected dimensions, paths and types, statistics, and lifecycle across billions of file records. It serves the same coarse planning role as Iceberg metadata; Uber's implementation is not current OSS.

Uber also uses that metadata for **early termination** on eligible newest-first `ORDER BY timestamp DESC LIMIT N` queries. After pruning files that cannot match, the provider stops generating older splits only when trustworthy bounds prove they cannot affect the result. This is an Uber production extension, not current OSS behavior.

The second metadata scale lives inside each selected archive split. Parquet Variant provides a row-local `metadata` and `value` representation plus ordinary statistics for shredded columns. It does not define CLP's complete path/type-to-column map across recurring schemas, together with dictionaries, logtypes, and indexes. CLP places this information near the front of the archive, allowing a native worker to bind exact paths or typed wildcard value searches to encoded columns and search the object stream without staging the archive locally. Typed wildcard value search is current OSS; key discovery and general `{path, value}` projection remain future work.

Archive-local metadata also suggests a future semantic-pushdown boundary: a split could return fields matching structural criteria or compact schema/logtype context before expanding events. Presto could merge those summaries or narrow encoded regions. This is not current or benchmarked OSS behavior.

## Starting an Out-of-Band Presto Native Connector from Scratch

Building another out-of-band connector starts with the coordinator/worker contract. Decide the tables, types, splits, pushdowns, and residual behavior first; those choices determine the handles that must cross the boundary. The result is a coordinated release of a Java plugin, native library, and serialized protocol—not one binary.

**Implement planning in Java.** The plugin should publish its connector factory, functions, metadata, split manager, and any connector optimizer. CLP's [`ClpPlugin`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/presto-connector/src/main/java/com/facebook/presto/plugin/clp/ClpPlugin.java) and [`ConnectorCodecProvider`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/presto-connector/src/main/java/com/facebook/presto/plugin/clp/codec/ClpConnectorCodecProvider.java) show how table, column, split, and transaction handles are exposed and encoded. The coordinator must enable `use-connector-provided-serialization-codecs` so the worker receives that connector-specific representation.

**Implement execution behind one native entry point.** CLP exports [`registerExtensions`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/velox-connector/src/ClpPluginEntry.cpp), which registers both the Velox factory and the adapter that converts serialized handles into native objects. A linker version script exports only that entry point; the worker discovers the bundle through `plugin.dir`. This keeps plugin dependencies from colliding with libraries already present in the process.

**Release a compatible bundle.** CLP builds on `manylinux_2_28` for `amd64` and `arm64`, stages the coordinator JAR separately from the worker `.so`, bundles non-system libraries under `lib/`, and sets RUNPATH to `$ORIGIN/lib`. The staged payload feeds OS packages, tarballs, and an init-container image. Compatibility still includes libc, Presto Native, Velox, and the complete target-CPU flags. Folly's `F14LinkCheck` enforces its CPU-intrinsics mode across `dlopen`, so the [build](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/velox-connector/cmake/DeriveTargetCpuFlags.cmake) reuses Velox's `get_cxx_flags` helper.

**Test the shipped boundary.** Use Presto's query-runner infrastructure for catalog discovery, splits, planning, codecs, and handle serialization; keep native helpers independent of Presto Native and Velox where practical. Plugin registration, deserialization, data sources, and vectors require a real worker because that process supplies their symbols. CLP's cumulative [integration-test stack](https://github.com/y-scope/clp-plugin-presto-connector/pull/64) makes `task integration-tests:run` build the installer and launch a Java coordinator and native worker over real local and S3 archive fixtures. It tests the release artifacts, not a test-only linkage arrangement.

> **Publication check:** After the stack merges, replace the PR link with its stable integration-test documentation and confirm the release's Presto/Velox version, image names, and target-CPU matrix.

## One Presto, Different Storage Contracts

The CLP connector demonstrates a practical rule for Presto: choose the logical contract and physical layout separately. Iceberg Variant provides an interoperable lakehouse value and selective columnar acceleration through shredding. A specialized layout earns its place when it adds something material for the workload—for CLP, intrinsic columnization, recurring-shape locality, and log-specific encoding.

The open-source [CLP connector](https://github.com/y-scope/clp-plugin-presto-connector) is available today. Its out-of-band implementation works end to end over searchable KV-IR files and immutable archives. Current source includes mixed-format scans, typed exact-path access, whole-event reconstruction, typed wildcard value search across fields, predicate and projection pushdown, and pluggable metadata filtering.

At Uber, onboarded workloads produce approximately 25 PB of raw logs per month, compressed to roughly 750–850 TB—a 29–33× reduction. Five 48-core workers serve data directly from object storage. Advanced planning, early termination, and billion-file metadata scale are Uber extensions, not current public features.

Future PrestoDB Variant support could give users a common SQL surface without changing either storage model. The CLP connector could expose `event VARIANT` while still binding constant paths to CLP columns; schema-tree and pattern metadata may also support key discovery, structural summaries, or candidate narrowing before deeper analysis.

Variant and CLP therefore need not converge physically to coexist in Presto. Presto supplies the common planning, serialization, and connector boundaries that let each retain its strengths. One SQL engine can support different storage contracts without embedding every storage engine in the Presto codebase.

## Acknowledgments

YScope engineers led the connector implementation. IBM engineers and the Presto community supported infrastructure and review; YScope and Uber engineers extended and applied the architecture in production. Special thanks to everyone who helped turn dynamic loading and connector-specific serialization into reusable Presto Native infrastructure.
