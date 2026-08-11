# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

Presto gives storage systems with different physical strengths a common SQL surface. For semi-structured data, however, a common value type does not determine how records are organized, compressed, or searched.

Analytical platforms rarely control the applications producing logs, traces, audit records, or AI coding-agent sessions. Fields, nesting, and types evolve independently. JSON preserves that long tail but gives up field-level columnar execution. A global schema requires coordination the platform rarely has.

The open-source [CLP project](https://github.com/y-scope/clp) from [YScope](https://yscope.com) is a native columnar format and search engine for semi-structured data. Core ideas behind its storage model are described in the OSDI '24 paper [“μSlope: High Compression and Fast Search on Semi-Structured Logs”](https://www.usenix.org/conference/osdi24/presentation/wang-rui). YScope's August 2025 post, [“Querying CLP-compressed logs with SQL via Presto”](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82), explains the storage model, SQL access, pushdown, and original integration. That integration ran CLP natively in the Velox workers of a Presto fork, but required changes to the fork and Velox.

Variant has since become the lakehouse reference across SQL, table, and file layers, although PrestoDB does not yet support it end to end. That prompts two questions for Presto users: when the same semi-structured records could be stored through the Variant stack or CLP, how do the resulting layouts and scans differ? And how can CLP be exposed in Presto Native as a pluggable connector? This article begins with the storage question, then follows the connector from deployment to its coordinator–worker boundary.

## Variant in the Lakehouse Landscape

Variant is a typed dynamic value rather than a fixed record schema or JSON text. Rows may have different nested shapes while preserving scalar types. It standardizes what supporting systems exchange, not how nested fields are laid out for scanning.

The lakehouse Variant stack assigns each layer a role. A query engine supplies the SQL type and operators. [Trino](https://trino.io/docs/current/functions/variant.html) provides them today and its [Iceberg connector](https://trino.io/docs/current/connector/iceberg.html) connects the stack, making it useful precedent in the Presto lineage. [Iceberg v3](https://iceberg.apache.org/spec/#variant-type) defines the table type. Parquet defines the [`metadata` and `value` encoding](https://parquet.apache.org/docs/file-format/types/variantencoding/) and optional [shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/). The Parquet project's [overview](https://parquet.apache.org/blog/2026/02/27/variant-type-in-apache-parquet-for-semi-structured-data/) and Databricks' [introduction](https://www.databricks.com/blog/introducing-variant-new-open-standard-semi-structured-data-apache-parquettm-delta-lake) provide further background.

Unshredded Variant is a Parquet binary column of row-local dynamic values. It supports typed path navigation without parsing text, but inner fields lack independent Parquet encodings and statistics. Shredding promotes writer-selected paths such as `request.id` into typed columns for field-level compression, projection, statistics, and skipping. The `value` representation preserves everything else.

Selective shredding fits interoperable tables when writers know the hot paths before opening each file for writing. Changing that selection later requires rewriting the file. One `typed_value` column represents one declared type for a shredded field. Other types remain in `value`. Promoting every path and type would instead create a wide, sparse physical union with extra schema and metadata overhead.

## CLP: Native Columnar Semi-Structured Storage

YScope's [August 2025 post](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) gives the full introduction to Encoded Record Tables (ERTs), Merged Schema Trees (MSTs), logtypes, and the original Presto integration. The comparison here needs only the physical properties that distinguish CLP from Variant.

Parquet Variant is value-first: one table per file with row-level Variant values and selected shredded columns. CLP is column-first. A useful mental model is one archive containing many shape-specific columnar subtables behind a single logical event stream. Each immutable CLP archive independently defines its own ERTs. They are internal storage structures, not SQL tables. Each ERT stores records sharing one combination of keys and value types in homogeneous columns. The archive-local MST merges those ERT schemas into one inventory and maps each path and type to its physical columns.

Even comprehensively shredded Variant retains one file-wide schema, so a field absent from a row becomes a missing entry in an optional column. Parquet encodes that sparsity compactly through definition levels rather than literal null payloads. CLP instead represents structural absence by placing the record in an ERT without that field. Explicit source nulls remain preserved.

The MST is execution metadata local to the archive rather than a global table schema. An optional Presto table schema, called a **convenience schema**, can expose selected CLP paths such as `response.status` as ordinary typed columns. Only the SQL table interface changes. The ERT columns already exist, so the archive is not rewritten.

CLP also goes below the string boundary. It infers **logtypes**, format-string-like templates that encode recurring static text once, and stores the extracted variable values in separate typed ERT columns. Shredding a Variant path as a string leaves its contents whole unless another index parses it. When structure, static text, and values recur, ERT and logtype locality improve compression. The same locality narrows structural and text searches to relevant encoded columns and streams without reconstructing every string.

These advantages remain workload-dependent. CLP fits when the platform cannot control upstream schemas or preselect useful paths and needs cross-field or free-text search with compact archives that reduce object-storage transfer and request costs. Variant with selective shredding fits interoperable lakehouse tables with predictable hot paths. With one stable schema, a typed Parquet layout or comprehensively shredded Variant columnizes the same fields as CLP. With nearly unique shapes, CLP's recurring-shape locality fades. The benchmarks in YScope's August 2025 post predate Variant and Variant shredding. They compare CLP with conventional Parquet configurations, not the later Variant stack.

## From Predicate to Physical Scan: Parquet Variant and CLP

The earlier YScope walkthrough traces typed accessors through rewriting, split discovery, pushdown, and native execution. Here, the comparison begins after pushdown: which physical data must the reader touch?

| Predicate | Parquet Variant | CLP |
|---|---|---|
| Exact named path | Use a shredded column if selected, or traverse `value` otherwise | Resolve the path and type once per archive split, then scan matching ERT columns |
| Same path, multiple value types | Use `typed_value` for its declared type. Other types remain in `value` | Route each type to its own homogeneous ERT columns |
| Cross-field typed value search | Inspect each row's relevant `value` structure unless another index covers the search | Resolve candidate paths from archive schema metadata, then scan the corresponding encoded columns |

Exact paths can be efficient in either layout when Variant has shredded them. Otherwise, Variant inspects relevant `value` pages and each selected row's binary structure, while CLP resolves compatible ERT columns once per archive split. Current CLP predicates can also search typed values over wildcard keys.

Those scans depend on two metadata layers: a metastore that plans across files and metadata inside each selected CLP archive.

## Beyond File Pruning: Leveraging Metastore and CLP Metadata

Metastore planning is format-independent. Before creating splits, a provider can prune CLP storage objects—streaming-compressed, searchable key-value intermediate representation (KV-IR) files and compact immutable archives—using time bounds, dimensions, and statistics. Most analytical metastores support this. Uber's internal metastore applies it across billions of files with a modest operational footprint.

Trustworthy beginning and ending timestamps can enable **early termination** for eligible `ORDER BY timestamp DESC LIMIT N` queries. Uber combines those bounds with exact per-file matching counts for supported predicates and enumerates files by descending maximum timestamp. When the count first reaches `N`, the provider freezes the earliest timestamp covered and includes every file overlapping that watermark. It stops only when the next file is strictly older. Unsupported row-level predicates disable the guarantee. Time sorting and append-only publication help but are not required. General metastore interfaces rarely include this predicate-specific count and proof contract.

Once a file becomes a split, column statistics and indexes remain a conventional baseline. CLP also stores its complete path/type-to-column map plus logtype and value dictionaries near the archive front. These structures are not part of standard Parquet Variant. A worker can initialize from that compact metadata, identify candidate columns, and stream without local staging. The metastore chooses the files. CLP metadata narrows the work inside each archive.

In the future, the same compact metadata could return structural or logtype summaries or narrow candidates for semantic analysis before expanding events. Today, both metadata levels sit behind an ordinary Presto catalog. The operator path makes that abstraction concrete.

## Deploying and Querying with the CLP Connector

For an operator, the path is short: ingest with CLP, install the matched Java coordinator plugin and native worker bundle, enable the worker loader, and create `etc/catalog/clp.properties`. The [documented setup](https://docs.yscope.com/presto/connector/clp.html) reads MySQL metadata populated by the YScope CLP OSS package. Custom providers can replace it. The connector is read-only and exposes discovered datasets in its `default` schema. Its [packaging flow](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/tools/build-packages/README.md) supports OS packages, tarballs, and an init-container image.

Two Compose examples make the deployment concrete. The connector's [coordinator/native-worker stack](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/integration-tests/docker-compose.yaml) installs matched artifacts through the shipped installer image. The CLP [deployment stack](https://github.com/jackluo923/clp/blob/cf9e2fab8e307a90a1009364c7a21d7a7b0a1727/tools/deployment/presto-clp/docker-compose.yaml) places the coordinator and worker in a complete CLP environment.

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

Replace `application_logs` and the paths with a deployed dataset. Typed accessors state the expected runtime type. The wildcard-key predicate searches string fields without naming an exact key. Full JSON is reconstructed only for projected matches. YScope's [August 2025 walkthrough](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) covers rewriting and pushdown.

Behind that short operator path is a two-part coordinator–worker contract.

## Building Pluggable Presto Native Connectors: Lessons from CLP

An out-of-band connector for [Presto Native, documented under Presto C++](https://prestodb.io/docs/current/presto-cpp.html), must bridge two runtimes: the Java coordinator and Velox-based native workers. YScope engineers collaborated with the Presto community to build the CLP connector as one of the first end-to-end plugins across that boundary, making it a practical reference for this extension path. The earlier integration already ran CLP natively in workers, but integrated at the Velox execution-framework layer, with corresponding extensions in the forked Presto/Prestissimo codebase. It was an in-tree native integration, not an external plugin. The current connector instead integrates at the Presto Native worker-plugin boundary. It builds as a separate library against Presto and Velox headers, and each worker loads it out of band. Native execution moved out of the fork, not out of the worker.

Through [Presto's data-source model](https://prestodb.io/docs/current/overview/concepts.html), the connector exposes CLP as catalogs, tables, and splits. A query may span both CLP storage forms: appendable, streaming-compressed KV-IR files and compact immutable archives. Archive-specific structure remains a worker concern rather than a catalog-wide union:

~~~mermaid
flowchart TB
    client["SQL client"]
    coordinator["Java coordinator"]
    javaPlugin["CLP coordinator plugin<br/>Metadata · splits · handle serialization"]
    workers["Presto Native workers"]
    loader["Native plugin loader"]
    plugin["Out-of-band CLP connector .so<br/>Handle deserialization · scan"]
    storage["CLP storage<br/>Streaming-compressed KV-IR · compact immutable archives"]

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

**Define the coordinator–worker contract.** Start with tables, types, splits, supported pushdowns, residual behavior, and the handles that must cross the boundary. The Java side publishes the connector factory, metadata, and split manager, plus any functions or optimizer and the codecs for those handles. CLP's [`ClpPlugin`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/presto-connector/src/main/java/com/facebook/presto/plugin/clp/ClpPlugin.java) and [`ConnectorCodecProvider`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/presto-connector/src/main/java/com/facebook/presto/plugin/clp/codec/ClpConnectorCodecProvider.java) are concrete examples. The coordinator enables `use-connector-provided-serialization-codecs` so workers receive the connector-specific representation.

The native bundle exports one [`registerExtensions`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/velox-connector/src/ClpPluginEntry.cpp) entry point. It registers the Velox connector factory and the adapter that deserializes handles. The resulting data sources scan CLP and return columnar batches. A linker version script exposes only that entry point. `plugin.dir` tells the worker where to find the bundle. [Presto PR #27653](https://github.com/prestodb/presto/pull/27653) added factory registration from dynamically loaded code, while [PR #27652](https://github.com/prestodb/presto/pull/27652) added connector-specific native deserialization. Together, these hooks let the worker discover the factory and decode plans produced by Java. Execution stays in process, avoiding sidecar RPC and per-record language bridges.

**Package one compatible unit.** CLP builds on `manylinux_2_28` for `amd64` and `arm64`, stages the coordinator JAR separately from the worker `.so`, places non-system dependencies under `lib/`, and sets RUNPATH to `$ORIGIN/lib`. That payload feeds OS packages, tarballs, or an init-container image. Compatibility includes libc, Presto Native, Velox, and target-CPU flags. Folly's `F14LinkCheck` verifies its CPU-intrinsics mode across `dlopen`, so the [CLP build](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/velox-connector/cmake/DeriveTargetCpuFlags.cmake) reuses Velox's `get_cxx_flags` helper.

**Test the deployed boundary.** Presto's query-runner infrastructure covers catalog discovery, split planning, codecs, and handle serialization. Native helpers without Presto Native or Velox dependencies remain independently testable. Registration, deserialization, data sources, and vectors require a real worker because their symbols resolve at runtime. CLP's cumulative [integration-test stack](https://github.com/y-scope/clp-plugin-presto-connector/pull/64) makes `task integration-tests:run` build the installer and launch a Java coordinator and native worker over local and S3 archive fixtures.

For an external connector author, the reusable deliverable is the tested coordinator artifact, native bundle, and serialized protocol together.

## Conclusion: Choosing Semi-Structured Storage for Presto

There is no universal storage model for semi-structured data. The lakehouse Variant stack fits when cross-engine portability matters and writers know which paths deserve Parquet shredding. CLP fits when the analytical platform cannot control upstream schemas or content or predict which paths users will query. It intrinsically columnizes every observed path and type, groups recurring shapes in ERTs, and separates static text from variables. When records share structural patterns and repeat static text interleaved with variable values, homogeneous ERT columns and logtypes improve compression. Archive metadata narrows lookups to relevant columns and encoded streams. Stable schemas and nearly unique shapes reduce these advantages.

The open-source [CLP connector](https://github.com/y-scope/clp-plugin-presto-connector) brings CLP to PrestoDB through the pluggable Presto Native connector boundary. It supports typed exact-path queries, typed value search over wildcard keys, whole-event reconstruction, and pushdown across both CLP storage forms: streaming-compressed KV-IR files and compact immutable archives. At Uber, workloads produce about 25 PB of raw logs monthly, compressed to 750–850 TB, while five 48-core workers serve data from object storage. Advanced planning and early termination are Uber production extensions rather than current OSS features.

A future PrestoDB Variant type could give Parquet and CLP one SQL interface without making their scans alike. Parquet would read `value` or shredded columns. CLP would bind paths to ERT columns and encoded text streams. Presto's pluggable native boundary makes that distinction practical: Presto provides common SQL and planning, while each connector retains storage-specific execution outside the Presto codebase.

## Acknowledgments

At YScope, Jack Luo, Kirk Rodrigues, Rui Wang, Xiaochong Wei, and Chenxing Yang led the connector implementation. From IBM and the Presto community, Pratik Joseph Dabre, Matt Graham, Deepak Majeti, and Tim Meehan contributed infrastructure and review. At Uber, Karthik Bhat, Qingqin Hou, Curt Hu, Maksym Ivanchenko, Jianjian Xie, and Yun Zhang worked with YScope engineers to extend and apply the architecture in production. Special thanks to everyone who helped turn dynamic loading and connector-specific serialization into reusable Presto Native infrastructure.
