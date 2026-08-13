# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

Presto gives storage systems a common SQL interface, but common SQL does not make their physical layouts interchangeable. For semi-structured data, the storage format still determines how changing field paths and recurring text are organized, compressed, and searched.

This matters when an analytical platform cannot control the applications producing logs, traces, audit records, or AI coding-agent sessions. Field names, nesting, types, and message text can evolve independently. Storing each event as one JSON string preserves this variation but gives up field-level columnar execution. A global schema requires coordination the platform rarely has.

The open-source [Compressed Log Processor (CLP) project](https://github.com/y-scope/clp) from [YScope](https://yscope.com) provides a native columnar format and search engine for semi-structured data. At Uber, workloads onboarded to the CLP platform produce tens of petabytes of raw logs per month, with substantial headroom in the current deployment. The OSDI '24 paper [“μSlope: High Compression and Fast Search on Semi-Structured Logs”](https://www.usenix.org/conference/osdi24/presentation/wang-rui) describes CLP's storage model.

YScope's August 2025 blog, [“Querying CLP-compressed logs with SQL via Presto”](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82), covers the original storage model, SQL access, pushdown, and integration. That implementation ran natively in Presto Native's Velox-based workers, but the CLP code lived directly in the Presto Native and Velox source trees. Because those changes were not upstream, the deployment temporarily relied on a fork. The current connector retains native execution as a dynamically loaded worker plugin.

Variant has also become the lakehouse reference for semi-structured values. As of Q3 2026, PrestoDB does not yet support it end to end; [Iceberg v3 Variant reading is tracked upstream](https://github.com/prestodb/presto/issues/27198). Its engine, table, and file layers provide a useful comparison. We use that baseline to explain CLP's physical scan, then turn to metadata planning, deployment, and the Java–native connector boundary.

## Variant in the Lakehouse Landscape

Variant defines a typed dynamic value whose nested shape can vary by row. The lakehouse stack separates responsibilities: query engines provide the SQL type and operators, [Iceberg v3](https://iceberg.apache.org/spec/#variant-type) defines the table type, and Parquet defines the [`metadata` and `value` encoding](https://parquet.apache.org/docs/file-format/types/variantencoding/) and optional [shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/).

Unshredded Variant stores each row as a native binary value. Shredding gives writer-selected paths their own typed columns for compression, projection, statistics, and skipping, while `value` preserves the remaining structure. Writers must select those paths before opening the file. Adding them later requires rewriting it.

This gives lakehouse tables a portable representation while allowing writers to optimize predictable query paths. Specialized storage can take a different approach when neither paths nor types are predictable.

## CLP: Native Columnar Semi-Structured Storage

YScope's [August 2025 blog](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) explains the original CLP storage model, typed SQL access, split discovery, pushdown, and Presto integration. The [μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui) provides the formal background for Encoded Record Tables (ERTs), Merged Schema Trees (MSTs), and logtypes. Two physical properties matter for the comparison here: how CLP organizes document structure and how it decomposes recurring text.

At the document-structure level, Parquet Variant begins with row-local values and adds selected shredded columns. CLP begins with archive-local columns. Think of one CLP archive as many shape-specific columnar subtables behind a single logical event stream. Each immutable archive contains its own ERTs. These are internal storage structures, not SQL tables. Each ERT stores records sharing one combination of keys and value types in homogeneous columns. The archive-local MST combines those schemas and maps each path and type to its physical columns.

Because an ERT contains only the fields present in its record shape, CLP does not need one giant table containing every possible column, with nulls wherever a field is absent. Explicit nulls in the source data are preserved.

CLP also decomposes recurring text into format-string-like **log templates**, called logtypes. It stores recurring static text once and writes extracted variables to separate typed ERT columns. Repetition across structure, static text, and values improves compression and lets searches target relevant encoded columns and streams. A conventional string column needs a separate parser or index to exploit the same internal structure.

CLP gains the most when upstream schemas cannot be controlled and useful query paths cannot be predicted in advance, particularly when cross-field or free-text search and compact searchable archives matter. With one stable schema, a conventional typed columnar layout can provide the same field columns. When most record shapes or message patterns are unique, CLP has less repetition to exploit.

## From Predicate to Physical Scan: Parquet Variant and CLP

The earlier YScope walkthrough traces typed accessors through rewriting, split discovery, pushdown, and native execution. This comparison begins after pushdown and asks which physical data each format must scan.

| Predicate | Parquet Variant | CLP |
|---|---|---|
| Exact named path | Use a shredded column if selected, or traverse `value` otherwise | Resolve the path and type once per archive split, then scan matching ERT columns |
| Same path, multiple value types | Read the shredded field's declared `typed_value`; occurrences with other types remain in `value` | Route each type to its own homogeneous ERT columns |
| Cross-field typed value search | Inspect each row's relevant `value` structure unless another index covers the search | Use the archive MST to find all compatible typed columns, then scan them. The current connector supports this |
| Arbitrary wildcard key or path matching | Inspect row-local object metadata for unshredded paths not selected in advance | Resolve matching paths against the archive MST before scanning events. Broader connector support is being prepared for open-source release |
| Recurring-text or pattern search | Scan the relevant string values unless another index covers the predicate | Use log-template and variable dictionaries to rule out nonmatching patterns before seeking to relevant ERT columns and encoded streams |

## Beyond File Pruning: Leveraging Metastore and CLP Metadata

Queries can be narrowed in three stages. First, ordinary metastore pruning removes files that cannot match. Second, Uber's latest-`N` planning can stop selecting older files that may contain matches but cannot change the result. Third, once a CLP archive becomes a split, its archive-local metadata narrows the columns and encoded streams to read.

The native connector can scan two CLP storage forms when its split provider supplies them: compressed key-value intermediate representation (KV-IR) files, which are appendable, streamable, and searchable, and compact immutable archives. Both can be searched through streaming reads, while KV-IR can continue growing. Uber's internal metastore plans across both forms and applies the first two stages across billions of file records.

Presto represents `ORDER BY ... LIMIT` with a distributed Top-N. In the conventional Hive-style path, Top-N limits the result, not the scan: it sits above split discovery and cannot tell the split source when enough data has been found. Every file that survives ordinary pruning remains eligible because an unseen file may still contain a newer qualifying row. At log scale, a small `LIMIT` can therefore still require a large scan.

At Uber, the metastore can stop enumerating files early for eligible Top-N queries without changing the result. This differs from pruning: pruning asks whether a particular file can match, whereas the Top-N optimization asks whether any remaining older file could change the latest `N` results. A skipped file may contain matches, but none recent enough to enter the result.

After ordinary pruning, Uber's metastore receives candidate files ordered by maximum timestamp, newest first. For filters such as application or host, it can use exact per-file matching-event counts when the metadata provides them. Once the cumulative count reaches `N`, the lowest event-time bound among the files considered so far becomes a **watermark**. The metastore keeps every file whose event-time range overlaps that watermark and stops only when the next file's maximum timestamp falls below it. Because the candidates are ordered, the metastore can stop reading older metadata and creating splits. Workers never open those older files.

If any row-level filter prevents an exact per-file matching count, Uber keeps every file that remains after normal pruning. This optimization depends on accurate file time bounds and matching-event counts. Append-only, time-ordered files keep those bounds stable and often reduce overlap, making the optimization more effective, but those properties alone do not make the cutoff correct. This is an Uber production extension and is not available in the current open-source connector.

After a file becomes a split, ordinary column statistics and indexes can still narrow its scan. A CLP archive begins with compact metadata, including the archive-local MST and dictionaries for log templates and values. The MST provides the complete path/type-to-column map, while log-template metadata captures structure inside text values. A worker reads this metadata first, identifies candidate columns, and streams the relevant encoded data without staging the complete archive locally. The metastore chooses the files. CLP metadata narrows the work inside each archive.

Across Uber's current mix of CLP workloads, the enriched log records achieve a roughly 30× raw-to-compressed ratio. Those records include high-cardinality identifiers such as trace IDs, which contain little repetition and compress poorly. A handful of Presto Native workers query the resulting CLP data directly from object storage. These figures describe Uber's production deployment, not a controlled benchmark against Parquet Variant or another format. To Presto, this still appears as an ordinary catalog: the metadata provider selects files, and CLP narrows each selected archive.

## Deploying and Querying with the CLP Connector

Presto users query CLP through ordinary SQL while the connector preserves its storage-native scan. A single query can combine exact typed paths, cross-field search, and whole-event reconstruction:

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

Replace `application_logs` and the paths with a deployed dataset. Typed accessors make the expected value type explicit. `CLP_WILDCARD_STRING_COLUMN()` searches string values across compatible fields without naming a field. Despite its name, it performs cross-field value search rather than the broader arbitrary wildcard key/path matching shown in the comparison table. The whole event is reconstructed as JSON only for projected matches. An optional Presto table schema, called a **convenience schema**, can expose frequently used paths as ordinary typed columns.

Future PrestoDB Variant support could give CLP paths more natural SQL access without replacing CLP's physical storage model. YScope's [August 2025 walkthrough](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) covers the existing rewriting and pushdown mechanics.

For an operator, the deployment path is short: ingest with CLP and install the version-matched Java coordinator plugin and native worker bundle. On the coordinator, enable `use-connector-provided-serialization-codecs`. On each worker, point `plugin.dir` to the native bundle. Install matching `etc/catalog/clp.properties` files on the coordinator and workers.

The [documented setup](https://docs.yscope.com/presto/connector/clp.html) uses the MySQL-backed metadata provider populated by YScope's open-source CLP package. Metadata and split discovery sit behind provider interfaces. A deployment can implement and wire those interfaces for another metastore without changing the native scan. The connector is read-only and exposes discovered datasets in its `default` schema. Its [packaging flow](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/tools/build-packages/README.md) supports OS packages, tarballs, and an init-container image.

Two Docker Compose configurations illustrate the setup. The connector's [coordinator-and-native-worker stack](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/integration-tests/docker-compose.yaml) installs version-matched artifacts through the shipped installer image. The CLP [deployment stack](https://github.com/y-scope/clp/blob/main/tools/deployment/presto-clp/docker-compose.yaml) places the coordinator and worker in a complete CLP environment.

Behind this deployment is a contract between the Java coordinator plugin and the native worker plugin.

## Building Pluggable Presto Native Connectors: Lessons from CLP

[Presto Native](https://prestodb.io/docs/current/presto-cpp.html), Presto's C++ worker implementation, executes connector scans in Velox-based workers. An out-of-band connector therefore needs a Java coordinator half and a dynamically loaded native worker half. YScope engineers collaborated with the Presto community to build CLP as one of the first end-to-end plugins across that boundary.

The earlier integration already ran CLP natively, but it entered at the Velox execution layer and lived directly in the Presto Native and Velox source trees. Because those changes were not upstream, the deployment used a fork. It was an in-tree integration, not an external plugin. The native half of the current connector instead integrates with Presto Native at the worker-plugin boundary: a separate shared library built against Presto and Velox headers and loaded out of band. Native execution moved out of the fork, not out of the worker.

At this boundary, the Java plugin supplies table metadata, splits, and connector-specific handles to the Presto coordinator. Presto serializes those handles into the plan it schedules on native workers. On each worker, the connector adapter decodes the handles and the dynamically loaded plugin performs the storage-specific scan. Presto carries this information across the boundary, so the connector does not need its own Java-to-native RPC. When the split provider supplies both storage forms, a query can span compressed KV-IR files and immutable archives without forcing every observed path and type into one global Presto table schema:

![Coordinator–worker boundary for the pluggable CLP connector](images/presto-native-clp-connector-architecture.svg)

Three lessons from CLP apply to other external Presto Native connectors.

**Define the coordinator–worker contract.** Begin by listing every connector-specific object that crosses the boundary. For CLP, these are table, table-layout, column, split, and transaction handles, along with pushed-down operations and filters left for Presto to evaluate. This inventory determines the Java codecs, native handle adapters, and data-source interfaces the connector must implement.

CLP's [`ClpPlugin`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/presto-connector/src/main/java/com/facebook/presto/plugin/clp/ClpPlugin.java) and [`ConnectorCodecProvider`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/presto-connector/src/main/java/com/facebook/presto/plugin/clp/codec/ClpConnectorCodecProvider.java) show the Java side of this contract. On the native side, [`registerExtensions`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/velox-connector/src/ClpPluginEntry.cpp) registers the connector factory and handle adapter. A linker version script exposes only that entry point, and `plugin.dir` points the worker to the packaged library.

Two Presto hooks support this contract. [PR #27653](https://github.com/prestodb/presto/pull/27653) allows the worker loader to register a connector factory from dynamically loaded code. On the coordinator, `use-connector-provided-serialization-codecs` selects the connector's handle codecs. [PR #27652](https://github.com/prestodb/presto/pull/27652) routes those serialized handles to the corresponding native adapter. Execution remains inside the worker process, avoiding a sidecar service or a per-record language boundary.

**Package one compatible unit.** CLP builds on `manylinux_2_28` for `amd64` and `arm64`, stages the coordinator JAR and worker `.so`, puts native dependencies under `lib/`, and sets RUNPATH to `$ORIGIN/lib`. The same payload can be delivered as OS packages, a tarball, or an init-container image. The bundle must match libc, Presto Native, Velox, and CPU flags. Folly's `F14LinkCheck` verifies CPU-intrinsics mode across `dlopen`, so the [CLP build](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/velox-connector/cmake/DeriveTargetCpuFlags.cmake) reuses Velox's `get_cxx_flags` helper.

**Test the deployed boundary.** Java unit tests cover predicate conversion, plan rewriting, metadata and split listing, and codec round trips. Native helpers that do not depend on Presto or Velox runtime symbols can be tested separately. The plugin deliberately leaves worker-owned symbols for the host process to resolve, so registration, deserialization, data sources, and vectors are exercised through a real worker. In CLP's [integration-test stack](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/integration-tests/README.md), `task integration-tests:run` installs both artifacts and runs SQL through a Java coordinator and native worker against archive and KV-IR fixtures.

For an external connector, these pieces form one release unit: the tested coordinator artifact, native bundle, and serialized protocol.

## Conclusion: Choosing Semi-Structured Storage for Presto

Choose the lakehouse Variant stack when cross-engine portability and standard table semantics matter most. Shredding is most effective when writers can identify important paths before writing. Choose CLP when the analytical platform cannot control upstream schemas or content, recurring structure and text can improve compression and search, or queries must span fields not known at ingestion. Stable schemas and nearly unique shapes reduce CLP's advantages.

Future PrestoDB Variant operators could give CLP a standard dynamic SQL interface without replacing its ERT- and log-template-based execution. A common SQL interface need not imply a common physical layout. Through pluggable native connectors, Presto can own planning and distributed execution while storage engines preserve the encodings, metadata, and scan paths built for their workloads.

## Acknowledgments

At YScope, Jack Luo, Kirk Rodrigues, Rui Wang, Xiaochong Wei, and Chenxing Yang worked together to implement the connector. From IBM and the Presto community, Pratik Joseph Dabre, Matt Graham, Deepak Majeti, and Tim Meehan contributed infrastructure and review. At Uber, Karthik Bhat, Qingqin Hou, Maksym Ivanchenko, and Yun Zhang from the Observability team worked with YScope engineers to extend and apply the architecture in production. Curt Hu and Jianjian Xie from Uber's Presto team supported the Presto integration. Jianjian also supported the connector's deployment and ongoing maintenance at Uber. Special thanks to everyone who helped turn dynamic loading and connector-specific serialization into reusable Presto Native infrastructure.
