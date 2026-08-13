# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

Presto gives storage systems a common SQL interface, but common SQL does not make their physical layouts interchangeable. For semi-structured data, the format still determines how evolving paths and recurring text are organized, compressed, and searched.

This matters when an analytical platform cannot control the applications producing logs, traces, audit records, or AI coding-agent sessions. Fields, nesting, types, and messages evolve independently. JSON preserves that long tail but gives up field-level columnar execution. A global schema requires coordination the platform rarely has.

The open-source [CLP project](https://github.com/y-scope/clp) from [YScope](https://yscope.com) addresses that physical problem with a native columnar format and search engine for semi-structured data. At Uber, CLP serves tens of petabytes of raw logs per month, with substantial headroom in the current deployment. At this scale, storage layout directly affects cost and query performance. Core ideas behind CLP's storage model are described in the OSDI '24 paper [“μSlope: High Compression and Fast Search on Semi-Structured Logs”](https://www.usenix.org/conference/osdi24/presentation/wang-rui).

YScope's August 2025 blog, [“Querying CLP-compressed logs with SQL via Presto”](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82), covers the original storage model, SQL access, pushdown, and integration. That implementation already ran natively in Presto Native's Velox-based workers, but the CLP integration lived directly in the Presto Native and Velox source trees. Because those changes were not upstream, the deployment temporarily carried a fork. The current connector keeps execution native while moving the integration to a dynamically loaded worker plugin.

Variant has also become the lakehouse reference for semi-structured values, although PrestoDB does not yet support it end to end. Its engine, table, and file layers give us a useful comparison baseline. We first use that baseline to explain how CLP changes the physical scan, then follow those choices through metadata planning, deployment, and the Java–native connector boundary.

## Variant in the Lakehouse Landscape

Variant standardizes a typed dynamic value that can have a different nested shape in each row. The lakehouse stack separates responsibilities: query engines provide the SQL type and operators, [Iceberg v3](https://iceberg.apache.org/spec/#variant-type) defines the table type, and Parquet defines the [`metadata` and `value` encoding](https://parquet.apache.org/docs/file-format/types/variantencoding/) and optional [shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/).

Unshredded Variant stores each row as a native binary value. Shredding gives writer-selected paths independent typed columns for compression, projection, statistics, and skipping, while `value` preserves the remaining structure. Writers must select those paths before opening the file; adding them later requires rewriting it.

This gives lakehouse tables a portable representation while allowing writers to optimize predictable query paths. The next question is what specialized storage can do when those paths and types cannot be predicted.

## CLP: Native Columnar Semi-Structured Storage

YScope's [August 2025 blog](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) explains the original CLP storage model, typed SQL access, split discovery, pushdown, and Presto integration. The [μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui) provides the formal background for Encoded Record Tables (ERTs), Merged Schema Trees (MSTs), and logtypes. Two physical properties matter for the comparison here: how CLP organizes document structure and how it decomposes recurring text.

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
| Wildcard key or path matching | Inspect row-local object metadata for unshredded paths not selected in advance | Resolve matching paths against the archive MST before scanning events; broader connector support is being prepared for open-source release |
| Recurring-text or pattern search | Scan the relevant string values unless another index covers the predicate | Use logtype and variable dictionaries to rule out nonmatching patterns before seeking to relevant ERT columns and encoded streams |

## Beyond File Pruning: Leveraging Metastore and CLP Metadata

Queries can be narrowed in three stages. First, ordinary metastore pruning removes files that cannot match. Second, Uber's query-aware latest-`N` planning can stop selecting older files that may contain matches but cannot change the result. Third, once a CLP archive becomes a split, its archive-local metadata narrows the columns and encoded streams to read.

The metastore plans across CLP's two storage forms: compressed key-value intermediate representation (KV-IR) files, which are appendable, streamable, and searchable, and compact immutable archives. Both support streaming search. Uber's internal metastore applies the first two stages across billions of file records.

Presto represents `ORDER BY ... LIMIT` with a distributed Top-N. In the conventional Hive-style path, Top-N limits the result, not the scan: it sits above split discovery and cannot tell the split source when enough data has been found. Every file that survives ordinary pruning remains eligible because an unseen file may still contain a newer qualifying row. At log scale, a small `LIMIT` can therefore leave a large scan.

At Uber, the metastore adds a correctness-preserving planning optimization that stops file enumeration early for eligible Top-N queries. This is different from pruning. Pruning asks whether one file can match. The Top-N optimization asks whether any remaining older file could change the latest `N` results. A skipped file may still contain matches, but none new enough to enter the result.

After ordinary pruning, Uber's metastore receives candidate files in descending order of their maximum timestamps. It can account for filters such as application or host when file metadata provides exact matching-event counts. Once the accumulated count reaches `N`, the minimum event-time bound among the files considered so far becomes a **watermark**. The metastore keeps every file whose event-time range overlaps that watermark and stops only when the next file's maximum timestamp is earlier. Because candidates arrive in order, it can stop consuming older metadata as well as stop creating splits. Workers never open those older files.

If any row-level filter prevents an exact per-file matching count, Uber keeps every file that remains after normal pruning. This optimization depends on accurate file time bounds and matching-event counts. Append-only, time-ordered files keep those bounds stable and often reduce overlap, making the optimization more effective, but those properties alone do not make the cutoff correct. This is an Uber production extension, not a current open-source connector feature.

After a file becomes a split, ordinary column statistics and indexes can still narrow its scan. A CLP archive begins with compact metadata, including the archive-local MST and dictionaries for log templates and values. The MST provides the complete path/type-to-column map, while log-template metadata captures structure inside text values. A worker reads this metadata first, identifies candidate columns, and streams the relevant encoded data without staging the complete archive locally. The metastore chooses the files. CLP metadata narrows the work inside each archive.

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

For an operator, the deployment path is short: ingest with CLP and install the matched Java coordinator plugin and native worker bundle. On the coordinator, enable `use-connector-provided-serialization-codecs`. On each worker, point `plugin.dir` to the native bundle, then install matching `etc/catalog/clp.properties` files. The [documented setup](https://docs.yscope.com/presto/connector/clp.html) uses the MySQL-backed metadata provider populated by the YScope CLP OSS package. Metadata access sits behind provider interfaces, allowing deployments to connect their own metastore and planning logic without coupling the native scan to MySQL. The connector is read-only and exposes discovered datasets in its `default` schema. Its [packaging flow](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/tools/build-packages/README.md) supports OS packages, tarballs, and an init-container image.

Two Compose configurations show the setup. The connector's [coordinator/native-worker stack](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/integration-tests/docker-compose.yaml) installs matched artifacts through the shipped installer image. The CLP [deployment stack](https://github.com/jackluo923/clp/blob/cf9e2fab8e307a90a1009364c7a21d7a7b0a1727/tools/deployment/presto-clp/docker-compose.yaml) places the coordinator and worker in a complete CLP environment.

That short operator path depends on a two-part coordinator–worker contract.

## Building Pluggable Presto Native Connectors: Lessons from CLP

An out-of-band connector for [Presto Native, documented under Presto C++](https://prestodb.io/docs/current/presto-cpp.html), bridges the Java coordinator and Velox-based workers. YScope engineers collaborated with the Presto community to build CLP as one of the first end-to-end plugins across that boundary.

The earlier integration already ran CLP natively, but it entered at the Velox execution layer and depended on extensions in forked Presto/Prestissimo and Velox. It was an in-tree integration, not an external plugin. The current connector instead integrates with Presto Native as a worker plugin: a separate shared library built against Presto and Velox headers and loaded out of band. Native execution moved out of the fork, not out of the worker.

At this boundary, the Java plugin supplies table metadata, splits, and connector-specific handles to the Presto coordinator. Presto serializes those handles into the plan it schedules on native workers. On each worker, the connector adapter decodes the handles and the dynamically loaded plugin performs the storage-specific scan. Presto—not a connector-owned Java-to-native RPC—carries information across this boundary. A query can therefore span compressed KV-IR files and immutable archives without forcing every observed path and type into one global Presto table schema:

![Coordinator–worker boundary for the pluggable CLP connector](images/presto-native-clp-connector-architecture.svg)

Building CLP exposed three lessons that apply to other external Presto Native connectors.

**Define the coordinator–worker contract.** Start by listing everything that must cross from the Java coordinator to a native worker: connector, table, column, and split handles; types; pushed-down operations; and filters Presto must still evaluate. The Java plugin discovers tables and plans splits, then serializes the connector-specific handles. On each worker, the loaded library registers its connector factory and handle adapter through [`registerExtensions`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/velox-connector/src/ClpPluginEntry.cpp). The adapter decodes those handles and creates the native data source.

CLP's [`ClpPlugin`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/presto-connector/src/main/java/com/facebook/presto/plugin/clp/ClpPlugin.java) and [`ConnectorCodecProvider`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/presto-connector/src/main/java/com/facebook/presto/plugin/clp/codec/ClpConnectorCodecProvider.java) show the Java side of this contract. A linker version script exposes only `registerExtensions`, and `plugin.dir` points the worker to the packaged library.

Two Presto hooks make the round trip possible. [PR #27653](https://github.com/prestodb/presto/pull/27653) allows the worker loader to register a connector factory from dynamically loaded code. On the coordinator, `use-connector-provided-serialization-codecs` selects the connector's handle codecs. [PR #27652](https://github.com/prestodb/presto/pull/27652) routes those serialized handles to the corresponding native adapter. Execution remains inside the worker process, avoiding a sidecar service or a per-record language boundary.

**Package one compatible unit.** CLP builds on `manylinux_2_28` for `amd64` and `arm64`, stages the coordinator JAR and worker `.so`, puts native dependencies under `lib/`, and sets RUNPATH to `$ORIGIN/lib`. That payload feeds packages, tarballs, or an init-container image. Compatibility includes libc, Presto Native, Velox, and CPU flags. Folly's `F14LinkCheck` verifies CPU-intrinsics mode across `dlopen`, so the [CLP build](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/velox-connector/cmake/DeriveTargetCpuFlags.cmake) reuses Velox's `get_cxx_flags` helper.

**Test the deployed boundary.** Java unit tests cover SQL-to-KQL conversion, plan rewriting, metadata and split listing, and codec round trips. Dependency-free native helpers remain independently testable. Registration, deserialization, data sources, and vectors need a real worker because their symbols resolve at runtime. CLP's [integration-test stack](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/integration-tests/README.md) makes `task integration-tests:run` install both artifacts and run SQL through a Java coordinator and native worker over archive and KV-IR fixtures.

For an external connector, these pieces form one release unit: the tested coordinator artifact, native bundle, and serialized protocol.

## Conclusion: Choosing Semi-Structured Storage for Presto

Choose the lakehouse Variant stack when cross-engine portability and standard table semantics matter most. Shredding is most effective when writers can identify important paths before writing. Choose CLP when the analytical platform cannot control upstream schemas or content, recurring structure and text can improve compression and search, or queries must span fields not known at ingestion. Stable schemas and nearly unique shapes reduce CLP's advantages.

Future PrestoDB Variant operators could give CLP a standard dynamic SQL interface without replacing its ERT- and log-template-based execution. The broader Presto lesson is that a common SQL interface does not require a common physical layout. Through pluggable native connectors, Presto can own planning and distributed execution while storage engines preserve the encodings, metadata, and scan paths built for their workloads.

## Acknowledgments

At YScope, Jack Luo, Kirk Rodrigues, Rui Wang, Xiaochong Wei, and Chenxing Yang led the connector implementation. From IBM and the Presto community, Pratik Joseph Dabre, Matt Graham, Deepak Majeti, and Tim Meehan contributed infrastructure and review. At Uber, Karthik Bhat, Qingqin Hou, Curt Hu, Maksym Ivanchenko, Jianjian Xie, and Yun Zhang worked with YScope engineers to extend and apply the architecture in production. Special thanks to everyone who helped turn dynamic loading and connector-specific serialization into reusable Presto Native infrastructure.
