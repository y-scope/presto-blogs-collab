# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

Presto gives storage systems a common SQL interface, but common SQL does not make their physical layouts interchangeable. For semi-structured data, the format still determines how evolving paths and recurring text are organized, compressed, and searched.

This matters when an analytical platform cannot control the applications producing logs, traces, audit records, or AI coding-agent sessions. Fields, nesting, types, and messages evolve independently. JSON preserves that long tail but gives up field-level columnar execution. A global schema requires coordination the platform rarely has.

The open-source [CLP project](https://github.com/y-scope/clp) from [YScope](https://yscope.com) addresses that physical problem with a native columnar format and search engine for semi-structured data. At Uber, CLP serves tens of petabytes of raw logs per month, with substantial headroom in the current deployment. At this scale, storage layout directly affects cost and query performance. Core ideas behind CLP's storage model are described in the OSDI '24 paper [“μSlope: High Compression and Fast Search on Semi-Structured Logs”](https://www.usenix.org/conference/osdi24/presentation/wang-rui).

YScope's August 2025 blog, [“Querying CLP-compressed logs with SQL via Presto”](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82), covers the original storage model, SQL access, pushdown, and integration. That implementation ran natively at the Velox execution layer in a Presto fork. CLP is now available through a pluggable Presto Native connector.

Variant has also become a useful lakehouse reference for semi-structured values, although PrestoDB does not yet support it end to end. Its separation of engine type, table contract, and file encoding provides a useful reference for understanding CLP's different physical choices.

## Variant in the Lakehouse Landscape

Variant standardizes a typed dynamic value that can have a different nested shape in each row. The lakehouse stack separates responsibilities: query engines provide the SQL type and operators, [Iceberg v3](https://iceberg.apache.org/spec/#variant-type) defines the table type, and Parquet defines the [`metadata` and `value` encoding](https://parquet.apache.org/docs/file-format/types/variantencoding/) and optional [shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/).

Unshredded Variant stores each row as a native binary value. Shredding gives writer-selected paths independent typed columns for compression, projection, statistics, and skipping, while `value` preserves the remaining structure. Writers must select those paths before opening the file; adding them later requires rewriting it.

This gives lakehouse tables a portable representation while allowing writers to optimize predictable query paths. The next question is what specialized storage can do when those paths and types cannot be predicted.

## CLP: Native Columnar Semi-Structured Storage

YScope's [August 2025 blog](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) gives the full introduction to Encoded Record Tables (ERTs), Merged Schema Trees (MSTs), log templates—called logtypes in CLP—and the original Presto integration. Two ideas are enough to understand the current connector: how CLP organizes document structure and how it decomposes recurring text.

At the document-structure level, Parquet Variant begins with row-local values and adds selected shredded columns. CLP begins with archive-local columns. Think of one CLP archive as many shape-specific columnar subtables behind a single logical event stream. Each immutable archive defines its own ERTs. These are internal storage structures, not SQL tables. Each ERT stores records sharing one combination of keys and value types in homogeneous columns. The archive-local MST combines those schemas and maps each path and type to its physical columns.

Because an ERT contains only the fields present in its record shape, CLP does not need one giant table containing every possible column, with nulls wherever a field is absent. Explicit source nulls remain preserved.

CLP also decomposes recurring text. It infers format-string-like **log templates**, encodes their recurring static text once, and stores extracted variables in separate typed ERT columns. When structure, static text, and values recur, this locality improves compression and narrows searches to relevant encoded columns and streams. A conventional string column would need another parser or index to exploit that internal structure.

CLP gains the most when upstream schemas and useful query paths cannot be controlled in advance, particularly when cross-field or free-text search and compact searchable archives matter. With one stable schema, a conventional typed columnar layout can provide the same field columns. With nearly unique shapes or text, CLP has less repetition to exploit.

## From Predicate to Physical Scan: Parquet Variant and CLP

The earlier YScope walkthrough traces typed accessors through rewriting, split discovery, pushdown, and native execution. This comparison begins after pushdown: which physical data must the reader touch?

| Predicate | Parquet Variant | CLP |
|---|---|---|
| Exact named path | Use a shredded column if selected, or traverse `value` otherwise | Resolve the path and type once per archive split, then scan matching ERT columns |
| Same path, multiple value types | Use `typed_value` for its declared type; other types remain in `value` | Route each type to its own homogeneous ERT columns |
| Cross-field typed value search | Inspect each row's relevant `value` structure unless another index covers the search | Use the archive MST to find all compatible typed columns, then scan them; supported by the current connector |
| Wildcard key or path matching | Inspect row-local object metadata for unshredded paths not selected in advance | Match the key pattern against the archive MST before scanning events; the connector interface is future work |

The metastore first narrows the files. Metadata inside each selected CLP archive then narrows the columns and encoded streams to read.

## Beyond File Pruning: Leveraging Metastore and CLP Metadata

Before creating splits, a metastore can use time bounds, dimensions, and statistics to prune CLP's two storage forms: compressed key-value intermediate representation (KV-IR) files that are appendable, streamable, and searchable, and compact immutable archives. Both support streaming search. Uber's internal metastore applies this planning across billions of file records.

Presto represents `ORDER BY ... LIMIT` with a distributed Top-N. In the conventional Hive-style path, Top-N limits the result, not the scan: it sits above split discovery and cannot tell the split source when enough data has been found. Every file that survives ordinary pruning remains eligible because an unseen file may still contain a newer qualifying row. At log scale, a small `LIMIT` can therefore leave a large scan.

Uber's metastore adds a planning optimization that stops file enumeration early for eligible Top-N queries. This is different from pruning. Pruning asks whether one file can match. The Top-N optimization asks whether any remaining older file could change the latest `N` results. A skipped file may still contain matches, but none new enough to enter the result.

After ordinary pruning, Uber's metastore examines the remaining files in descending order of their maximum timestamps. For filters it can resolve from file metadata, it adds the exact number of matching events in each file. Once that count reaches `N`, the earliest event timestamp covered so far becomes a **watermark**. The metastore keeps every file whose event-time range overlaps the watermark and stops only when the next file's maximum timestamp is earlier than the watermark. Those older files never become splits, so workers never open them.

If row-level filters prevent the metastore from knowing how many events match, it keeps every file that remains after normal pruning. This optimization depends on accurate file time bounds and matching-event counts. Append-only, time-ordered files keep those bounds stable and often reduce overlap, making the optimization more effective, but those properties alone do not make the cutoff correct. This is an Uber production extension, not a current open-source connector feature.

After a file becomes a split, ordinary column statistics and indexes can still narrow its scan. CLP adds the archive-local MST and dictionaries for log templates and values in the archive header. The MST provides the complete path/type-to-column map, while log-template metadata captures structure inside text values. A worker reads this compact metadata first, identifies candidate columns, and streams the relevant encoded data without staging the complete archive locally. The metastore chooses the files. CLP metadata narrows the work inside each archive.

Across Uber's current mix of CLP workloads, the enriched log records achieve a roughly 30× raw-to-compressed ratio. Those records include high-cardinality identifiers such as trace IDs, which contain little repetition and compress poorly. A handful of Presto Native workers query the resulting CLP data directly from object storage. These are observations from Uber's operating deployment, not a controlled benchmark against Parquet Variant or another format. The connector uses both metadata layers behind an ordinary Presto catalog.

## Deploying and Querying with the CLP Connector

Presto users access CLP's storage-native scans through ordinary SQL. A single query can combine exact typed paths, cross-field search, and complete-event reconstruction:

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

Replace `application_logs` and the paths with a deployed dataset. Typed accessors state the expected runtime type. `CLP_WILDCARD_STRING_COLUMN()` searches string values across compatible fields without naming one, and complete JSON is reconstructed only for projected matches. An optional Presto table schema, called a **convenience schema**, can expose frequently used paths as ordinary typed columns.

Future PrestoDB Variant support could replace the exact-path accessor syntax without changing CLP's path binding or physical scan. YScope's [August 2025 walkthrough](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) covers the existing rewriting and pushdown mechanics.

For an operator, the deployment path is short: ingest with CLP, install the matched Java coordinator plugin and native worker bundle, enable the worker loader, and create `etc/catalog/clp.properties`. The [documented setup](https://docs.yscope.com/presto/connector/clp.html) reads MySQL metadata populated by the YScope CLP OSS package. Custom metadata providers can replace the default MySQL-backed provider. The connector is read-only and exposes discovered datasets in its `default` schema. Its [packaging flow](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/tools/build-packages/README.md) supports OS packages, tarballs, and an init-container image.

Two Compose configurations show the setup. The connector's [coordinator/native-worker stack](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/integration-tests/docker-compose.yaml) installs matched artifacts through the shipped installer image. The CLP [deployment stack](https://github.com/jackluo923/clp/blob/cf9e2fab8e307a90a1009364c7a21d7a7b0a1727/tools/deployment/presto-clp/docker-compose.yaml) places the coordinator and worker in a complete CLP environment.

That short operator path depends on a two-part coordinator–worker contract.

## Building Pluggable Presto Native Connectors: Lessons from CLP

An out-of-band connector for [Presto Native, documented under Presto C++](https://prestodb.io/docs/current/presto-cpp.html), bridges the Java coordinator and Velox-based workers. YScope engineers collaborated with the Presto community to build CLP as one of the first end-to-end plugins across that boundary.

The earlier integration already ran CLP natively, but it entered at the Velox execution layer and depended on extensions in forked Presto/Prestissimo and Velox. It was an in-tree integration, not an external plugin. The current connector instead integrates with Presto Native as a worker plugin: a separate shared library built against Presto and Velox headers and loaded out of band. Native execution moved out of the fork, not out of the worker.

Through [Presto's data-source model](https://prestodb.io/docs/current/overview/concepts.html), the Java plugin presents CLP as catalogs, tables, and splits, while the native library retains archive-specific execution. A query may span compressed KV-IR files and compact immutable archives without turning their local structure into a catalog-wide union:

~~~mermaid
flowchart TB
    client["SQL client"]
    coordinator["Java coordinator"]
    javaPlugin["CLP coordinator plugin<br/>Metadata · splits · handle serialization"]
    workers["Presto Native workers"]
    loader["Native plugin loader"]
    plugin["Out-of-band CLP connector .so<br/>Handle deserialization · scan"]
    storage["CLP storage<br/>Compressed KV-IR: appendable · streamable · searchable<br/>Compact immutable archives"]

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

Building CLP exposed three lessons that apply to other external Presto Native connectors.

**Define the coordinator–worker contract.** Start with tables, types, splits, pushdowns, residuals, and cross-boundary handles. Java publishes the connector factory, metadata, split manager, functions or optimizer, and handle codecs. CLP's [`ClpPlugin`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/presto-connector/src/main/java/com/facebook/presto/plugin/clp/ClpPlugin.java) and [`ConnectorCodecProvider`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/presto-connector/src/main/java/com/facebook/presto/plugin/clp/codec/ClpConnectorCodecProvider.java) are examples. The native [`registerExtensions`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/velox-connector/src/ClpPluginEntry.cpp) entry point registers the Velox connector factory and handle adapter. `plugin.dir` locates the bundle, and a linker version script exposes only that entry point.

Two Presto hooks complete that contract. [PR #27653](https://github.com/prestodb/presto/pull/27653) added factory registration from dynamically loaded code. [PR #27652](https://github.com/prestodb/presto/pull/27652) added connector-specific native deserialization. With `use-connector-provided-serialization-codecs` enabled, workers can discover the native factory and decode handles produced by Java. Execution stays in process, avoiding sidecar RPC and per-record language bridges.

**Package one compatible unit.** CLP builds on `manylinux_2_28` for `amd64` and `arm64`, stages the coordinator JAR and worker `.so`, puts native dependencies under `lib/`, and sets RUNPATH to `$ORIGIN/lib`. That payload feeds packages, tarballs, or an init-container image. Compatibility includes libc, Presto Native, Velox, and CPU flags. Folly's `F14LinkCheck` verifies CPU-intrinsics mode across `dlopen`, so the [CLP build](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/velox-connector/cmake/DeriveTargetCpuFlags.cmake) reuses Velox's `get_cxx_flags` helper.

**Test the deployed boundary.** Query-runner tests cover catalog discovery, split planning, codecs, and handle serialization. Dependency-free native helpers remain independently testable. Registration, deserialization, data sources, and vectors need a real worker because their symbols resolve at runtime. CLP's cumulative [integration-test stack](https://github.com/y-scope/clp-plugin-presto-connector/pull/64) makes `task integration-tests:run` build the installer and launch a Java coordinator and native worker over local and S3 archive fixtures.

For an external connector, these pieces form one release unit: the tested coordinator artifact, native bundle, and serialized protocol.

## Conclusion: Choosing Semi-Structured Storage for Presto

Choose the lakehouse Variant stack when cross-engine portability and standard table semantics matter most. Shredding is most effective when writers can identify important paths before writing. Choose CLP when the analytical platform cannot control upstream schemas or content, recurring structure and text can improve compression and search, or queries must span fields not known at ingestion. Stable schemas and nearly unique shapes reduce CLP's advantages.

Future PrestoDB Variant operators could give CLP a standard dynamic SQL interface without replacing its ERT- and log-template-based execution. The open-source [CLP connector](https://github.com/y-scope/clp-plugin-presto-connector) shows that Presto can provide a common SQL interface and planning layer while CLP handles storage-specific native execution behind a pluggable connector.

## Acknowledgments

At YScope, Jack Luo, Kirk Rodrigues, Rui Wang, Xiaochong Wei, and Chenxing Yang led the connector implementation. From IBM and the Presto community, Pratik Joseph Dabre, Matt Graham, Deepak Majeti, and Tim Meehan contributed infrastructure and review. At Uber, Karthik Bhat, Qingqin Hou, Curt Hu, Maksym Ivanchenko, Jianjian Xie, and Yun Zhang worked with YScope engineers to extend and apply the architecture in production. Special thanks to everyone who helped turn dynamic loading and connector-specific serialization into reusable Presto Native infrastructure.
