# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

# Introduction

For decades, data had to earn its way into a centralized warehouse: it was fitted to a schema designed in advance and queried by the warehouse’s dedicated engine. As data exploded in volume and variety, the cost and rigidity of this schema-on-write model just could not scale accordingly. Enterprises began pouring data into cheap data lakes instead, where raw data lands in open formats such as Parquet. However, a lake alone only stores the data, it offers no data management for analytics. So the lakehouse architecture was defined; it adds a metadata abstraction such as Apache Iceberg on top of data lake to organizes the Parquet files into tables, then query engines such as Presto can query those tables directly, as they would a warehouse.

The lakehouse’s promise is one copy of data that any engine can read and write, and this promise has proven increasingly valuable. On the same Parquet files, users can run SQL queries for BI, or load the files as Arrow tables into ML frameworks such as Ray for model training. What the lakehouse leaves open is whether the physical layout beneath those tables should be unified as well. Our answer is no: a common table interface does not require a common physical layout, and a query engine can read each kind of data in the layout that fits it.

Take semi-structured data as an example. Each semi-structured record can carry its own schema: a field present in one record may be missing from another, or hold a value of a different type. Such data, mostly logs, metrics, and traces produced for observability, makes up a large share of what lands in a data lake. At Uber, roughly 25 PB of semi-structured data is produced every month.

The lakehouse’s answer for semi-structured data is Parquet Variant, a data type for values whose fields are not known in advance. A Variant column stores each semi-structured record in binary, encoded with field names, types, and values. Reading one field therefore requires decoding each row’s binary value, unless the field has been shredded, i.e., extracted into its own typed Parquet column. However, shredding requires configuring those fields before a file is written, which is impractical when the schema of semi-structured logs is inherently dynamic. Moreover, as of this writing, Presto’s native Iceberg support does not yet read Variant end to end; Iceberg v3 Variant reading is tracked upstream.

We propose CLP as a first-class format for semi-structured data in the data lake because it stores logs’ evolving fields in columns format without the manual configuration that Variant shredding requires. CLP does so by discovering each record’s structure at ingestion and grouping records that share a schema into tables for columnar look up. Like Parquet, CLP is an open format, but it takes much less space in object storage. CLP achieves a compression ratio 2.31X higher on average than the best Parquet configuration evaluated, while supporting search directly on the compressed data. For full lakehouse support, YScope’s CLP package manages CLP format into tables, as Iceberg does for Parquet files, and we have built a Presto CLP connector that lets Presto query those tables. YScope’s August 2025 blog describes the connector in detail.

With the storage side in place, how do we make CLP accessible to Presto on the compute side? Traditionally, the Presto accepted only in-tree connectors, i.e., connectors compiled into its source tree, such as Presto’s Iceberg connector. However, an in-tree connector is a long-term investment: Iceberg’s has taken over five years and roughly 900 commits, and is still growing. We raised this with the Presto community, and together we designed a plugin architecture: the connector ships as a separate artifact, and load it at runtime from a configured plugin directory. Presto CLP connector is the first connector to use it end to end; the required changes at PrestoDB upstream shipped in Presto 0.299 so that any connector can use them.

This architecture is already deployed in production at Uber, where a petabyte a day of semi-structured data are stored in CLP at about 30:1 compression and queried through Presto with thousands of SQL per day. The rest of this blog compares Parquet Variant and CLP, explains the technical details of the plugin, and describes how Uber adopted CLP in its lakehouse architecture.

## Variant in the Lakehouse Landscape

In the lakehouse, **Variant support** can mean support for a SQL type, a table-schema type, or a file encoding. These layers have different responsibilities: understanding Variant values in a query engine does not determine how they are stored or how the engine reads them.

SQL engines generally require each output column to have a known type before execution. Semi-structured data complicates this: the same field might contain an integer, a string, or an object across different records. Variant provides one SQL type that can hold these different values while preserving their individual types.

Consider a `logs` table with a Variant column named `event`. Each complete JSON object below is stored as one value in that column, one per table row. The `timestamp` and `error` fields are inside `event`, not separate table columns:

```jsonl
{"timestamp": "2026-10-19T12:00:00Z", "error": 504}
{"timestamp": "2026-10-19T12:00:00Z", "error": "connection timed out"}
{"timestamp": "2026-10-19T12:00:00Z", "error": {"code": 504, "message": "gateway timeout"}}
```

Three layers make these records queryable:

1. **Query-engine layer.** Variant is a SQL type, with operations to access its fields. In Databricks SQL, `:` accesses a field:

   ```sql
   SELECT event:error AS error FROM logs;
   ```

   The output column is Variant: the three results retain their integer, string, and object types without being coerced into strings.

2. **Table layer.** [Iceberg v3](https://iceberg.apache.org/spec/#semi-structured-types) records that `logs` has an `event` column of type Variant and tracks which files belong to the table. The connector exposes that column to the engine; the declaration does not list the fields inside each event.

3. **File-format layer.** Parquet defines the [binary representation](https://parquet.apache.org/docs/file-format/types/variantencoding/) of Variant values. The Parquet writer can extract fields into typed columns (“shredding”), and the reader reconstructs requested Variant values for the engine. Where supported, the connector passes filters to the reader so it can skip irrelevant data.

Together, these layers support dynamic records through a declared table schema, but Iceberg and Parquet are choices, not requirements for engine-level Variant support. An engine can expose Variant values through other table and storage formats. The CLP connector provides access to CLP’s tables and specialized scans, independently of whether the engine supports Variant.

For Parquet, binary Variant avoids parsing JSON text at query time, but extracting a field across many records can still be slow: the reader must navigate every relevant record’s binary value.

[Shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/) addresses this bottleneck by extracting selected fields into typed Parquet columns. Queries can then read those columns directly, skip irrelevant data using column statistics, and benefit from columnar compression.

Each field has only one shredded type per file. Shredding `error` as an integer puts `504` in an integer column. The string and object errors cannot fit that column, so they remain encoded as binary Variant values. Returning all three requires reading both the integer column and those binary values, preserving their original types.

The Parquet writer selects fields and types through [configuration or inference from sampled data](https://parquet.apache.org/blog/2026/02/27/variant-type-in-apache-parquet-for-semi-structured-data/). Changing those selections for an existing file requires rewriting it. The remaining challenge is to keep every record columnar as fields and types change.

## CLP: Native Columnar Semi-Structured File-Format

CLP addresses that challenge by discovering record schemas as data arrives. Within each compressed archive, it groups records with matching field paths and types into **Encoded Record Tables (ERTs)**. The three records above form three groups:

| Record schema shape | Columns in its ERT |
|---|---|
| Numeric error code | `timestamp`, `error` (integer) |
| Text error message | `timestamp`, `error` (string) |
| Structured error | `timestamp`, `error.code` (integer), `error.message` (string) |

A field path such as `error.code` identifies a field inside a nested object. Each ERT is an internal table containing only records with that schema; its columns therefore cover a subset of the archive’s records. New schema shapes form new ERTs. This keeps every record in typed columns without creating one wide table spanning every observed field.

An archive’s **Merged Schema Tree (MST)** acts as a map from field paths and types to these columns. The reader can locate `error`’s integer and string columns, or the column for `error.code`, before reading record values. The **metastore**, which tracks files belonging to each SQL table, does not track these record schemas; they live inside the archives.

CLP also separates recurring string values into **string shapes (templates)** and variables. For example, `request 123 timed out` and `request 456 timed out` share the illustrative template `request <request_count> timed out`, with `123` and `456` in a separate variable column. Storing repeated text once improves compression. Dictionaries of templates and repeated variable values also help narrow pattern searches before reading message values.

Meta’s [Nimble](https://github.com/facebookincubator/nimble), an alternative to Parquet and ORC, offers a related idea: [nested data streams](https://velox-lib.io/blog/nimble-stream-slicing/#nested-stream-mapping) need not have the same element count as the table’s row count. Nimble emphasizes wide datasets, extensible encodings, and parallel processing. CLP’s grouping, schema metadata, and text encodings are optimized for compression and search over dynamic records, as detailed in the [μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui).

## From Predicate to Physical Scan: Parquet Variant and CLP

The storage layout determines which columns or binary values the engine must read to evaluate a predicate.

Log analysis is a common use case for semi-structured data. For example, troubleshooting a failure may start with an error code or message pattern, before the field path is known. The code might appear at `error.code`, `response.error.code`, or `upstream.error.code`. Wildcard-path matching searches across these locations. Support depends on the query engine; the file format determines what it must read. The table compares that work using illustrative predicates over fields within `event`, with `504` matched as an integer:

| Predicate | Parquet Variant | CLP |
|---|---|---|
| `error = 504` | Read its integer column if shredded; otherwise inspect binary values. | Automatically use the schema map to locate and scan matching integer columns. |
| `*.error.* = 504` | Scan matching shredded integer columns. For unshredded data, read field names from Variant metadata and traverse each relevant record’s binary structure to match paths and integer types; then retrieve those integer values and compare them with `504`. | Automatically match `*.error.*` and integer types in the archive-wide MST, identify the ERTs containing those fields, then scan their integer columns for `504`. |
| `message LIKE 'request % timed out'` | Apply wildcard or regex matching to string values (or whole-string dictionary entries when supported). Variant neither stores nor exposes string templates. | Match string shapes in the dictionary, then scan the integer ID column for matching shape IDs; check variable values when needed. |

**CLP keeps dynamic records columnar across schema shapes; Parquet Variant’s field-level columnar access depends on shredding.** When all queried fields and types are [fully shredded](https://parquet.apache.org/docs/file-format/types/variantshredding/), both formats support direct typed-column scans.

For wildcard paths, CLP’s archive-wide MST identifies matching fields and the ERTs containing them before reading record values. The scan then reads only the relevant columns, avoiding the per-record path discovery required for unshredded Variant fields.

For string patterns, Parquet readers may need to test every remaining string value or [whole-string dictionary entry](https://parquet.apache.org/docs/file-format/data-pages/encodings/#dictionary-encoding-plain_dictionary--2-and-rle_dictionary--8), after any applicable pruning. CLP first matches shared string shapes, then scans integer shape IDs and checks variables as needed. This can replace repeated text matching with integer comparisons even when messages contain different variable values.

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

[Presto Native](https://prestodb.io/docs/current/presto-cpp.html), Presto's C++ worker implementation, executes connector scans in Velox-based workers. An out-of-band connector therefore needs a Java coordinator half and a dynamically loaded native worker half. YScope engineers collaborated with the Presto community to build CLP as the first end-to-end plugin across that boundary.

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
