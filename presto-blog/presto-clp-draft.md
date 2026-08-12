# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

Variant is standardizing how semi-structured values move through the lakehouse. Presto gives storage systems a common SQL surface, but neither a common value type nor a table contract determines how nested paths and recurring text are physically organized, compressed, or searched.

That distinction matters when an analytical platform cannot control the applications producing logs, traces, audit records, or AI coding-agent sessions. Fields, nesting, types, and messages evolve independently. JSON preserves that long tail but gives up field-level columnar execution. A global schema requires coordination the platform rarely has.

The open-source [CLP project](https://github.com/y-scope/clp) from [YScope](https://yscope.com) addresses that physical problem with a native columnar format and search engine for semi-structured data. At Uber, currently onboarded ML/AI, streaming, and data-platform workloads produce about 25 PB of raw logs per month, making storage layout a material concern rather than an implementation detail. Core ideas behind CLP's storage model are described in the OSDI '24 paper [“μSlope: High Compression and Fast Search on Semi-Structured Logs”](https://www.usenix.org/conference/osdi24/presentation/wang-rui).

YScope's August 2025 post, [“Querying CLP-compressed logs with SQL via Presto”](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82), covers the original storage model, SQL access, pushdown, and integration. That implementation ran natively at the Velox execution layer in a Presto fork. Today, CLP is available through a pluggable Presto Native connector, while PrestoDB does not yet support Variant end to end. This article compares their physical work, then follows the connector from SQL to its coordinator–worker boundary.

## Variant in the Lakehouse Landscape

Variant is a typed dynamic value rather than a fixed record schema or JSON text. Rows may have different nested shapes while preserving scalar types. It standardizes what supporting systems exchange, not how nested fields are laid out for scanning.

The lakehouse Variant stack assigns each layer a role. A query engine supplies the SQL type and operators. [Trino](https://trino.io/docs/current/functions/variant.html) provides them today and its [Iceberg connector](https://trino.io/docs/current/connector/iceberg.html) connects the stack, making it useful precedent in the Presto lineage. [Iceberg v3](https://iceberg.apache.org/spec/#variant-type) defines the table type. Parquet defines the [`metadata` and `value` encoding](https://parquet.apache.org/docs/file-format/types/variantencoding/) and optional [shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/). The Parquet project's [overview](https://parquet.apache.org/blog/2026/02/27/variant-type-in-apache-parquet-for-semi-structured-data/) and Databricks' [introduction](https://www.databricks.com/blog/introducing-variant-new-open-standard-semi-structured-data-apache-parquettm-delta-lake) provide further background.

Unshredded Variant is a Parquet binary column of row-local dynamic values. It supports typed path navigation without parsing text, but inner fields lack independent Parquet encodings and statistics. Shredding promotes writer-selected paths such as `request.id` into typed columns for field-level compression, projection, statistics, and skipping. The `value` representation preserves everything else.

Selective shredding fits interoperable tables when writers know the hot paths before opening each file for writing. Changing that selection later requires rewriting the file. One `typed_value` column represents one declared type for a shredded field. Other types remain in `value`. Promoting every path and type would instead create a wide, sparse physical union with extra schema and metadata overhead.

## CLP: Native Columnar Semi-Structured Storage

YScope's [August 2025 post](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) gives the full introduction to Encoded Record Tables (ERTs), Merged Schema Trees (MSTs), log templates—called logtypes in CLP—and the original Presto integration. Only two physical ideas need repeating here: how CLP organizes document structure and how it decomposes recurring text.

At the document-structure level, Parquet Variant is value-first: one table per file with row-level Variant values and selected shredded columns. CLP is column-first. A useful mental model is one archive containing many shape-specific columnar subtables behind a single logical event stream. Each immutable CLP archive independently defines its own ERTs. They are internal storage structures, not SQL tables. Each ERT stores records sharing one combination of keys and value types in homogeneous columns. The archive-local MST merges those schemas into one inventory and maps each path and type to its physical columns. It is execution metadata local to the archive, not a global table schema.

Even comprehensively shredded Variant retains one file-wide schema, so a field absent from a row becomes a missing entry in an optional column. Parquet encodes that sparsity compactly through definition levels rather than literal null payloads. CLP instead represents structural absence by placing the record in an ERT without that field. Explicit source nulls remain preserved.

CLP's second level of structurization is inside string values. It infers format-string-like **log templates** that encode recurring static text once and stores the extracted variable values in separate typed ERT columns. Shredding a Variant path as a string leaves its contents whole unless another index parses it. When structure, static text, and values recur, ERT and log-template locality improve compression. The same locality narrows structural and text searches to relevant encoded columns and streams without reconstructing every string.

These advantages remain workload-dependent. CLP fits when the platform cannot control upstream schemas or preselect useful paths and needs cross-field or free-text search with compact archives that reduce object-storage transfer and request costs. Variant with selective shredding fits interoperable lakehouse tables with predictable hot paths. With one stable schema, a typed Parquet layout or comprehensively shredded Variant columnizes the same fields as CLP. With nearly unique shapes, CLP's recurring-shape locality fades.

## From Predicate to Physical Scan: Parquet Variant and CLP

The earlier YScope walkthrough traces typed accessors through rewriting, split discovery, pushdown, and native execution. Here, the comparison begins after pushdown: which physical data must the reader touch?

| Predicate | Parquet Variant | CLP |
|---|---|---|
| Exact named path | Use a shredded column if selected, or traverse `value` otherwise | Resolve the path and type once per archive split, then scan matching ERT columns |
| Same path, multiple value types | Use `typed_value` for its declared type. Other types remain in `value` | Route each type to its own homogeneous ERT columns |
| Cross-field typed value search | Inspect each row's relevant `value` structure unless another index covers the search | Resolve candidate paths from the archive MST, then scan the corresponding encoded columns |

Exact paths can be efficient in either layout when Variant has shredded them. Otherwise, Variant inspects relevant `value` pages and each selected row's binary structure, while CLP resolves compatible ERT columns once per archive split. Current CLP predicates can also search typed values over wildcard keys.

Those scans depend on two metadata layers: a metastore that plans across files and metadata inside each selected CLP archive.

## Beyond File Pruning: Leveraging Metastore and CLP Metadata

Before creating splits, a metastore can use time bounds, dimensions, and statistics to prune CLP's two storage forms: compressed key-value intermediate representation (KV-IR) files that are appendable, streamable, and searchable, and compact immutable archives. Both support streaming search. Uber's internal metastore applies this format-independent planning across billions of files with a modest operational footprint.

Presto represents `ORDER BY ... LIMIT` with a distributed Top-N. In the conventional Hive-style path, Top-N limits the result, not the scan: it sits above split discovery and cannot tell the split source when enough data has been found. Every file that survives ordinary pruning remains eligible because an unseen file may still contain a newer qualifying row. At log scale, a small `LIMIT` can therefore leave a large scan.

Uber's provider avoids that behavior with a specialized form of **Top-N file-enumeration early termination**. This is not another name for file or split pruning. Pruning evaluates each candidate independently against the query predicates and removes it only when the file cannot contain a match. It does not depend on `ORDER BY` or `LIMIT`. Top-N termination makes a collective, result-level proof: older files may contain matching events, but none of them can displace the current newest `N`.

The provider first applies ordinary pruning. It then enumerates the surviving file metadata by descending maximum timestamp, accumulates exact matching counts for supported file-level predicates, and retains every file overlapping the cutoff timestamp. Once it can prove that the remaining older files cannot change the result, it stops producing splits for them. Those files never reach a worker or open an object-storage stream, even though some could match the filter. If the proof is unavailable, the provider safely falls back to the complete candidate set. Append-only publication and time ordering help establish the guarantee but are insufficient by themselves. Uber's forthcoming CLP Part III article (**[UBER_BLOG_URL](UBER_BLOG_URL)**) gives the production details. This optimization is an Uber production extension, not a current OSS connector feature.

Once a file becomes a split, column statistics and indexes remain a conventional baseline. CLP additionally stores its archive-local MST and dictionaries for log templates and values in the archive header. The MST provides the complete path/type-to-column map, while log-template metadata captures structure inside text values. A worker can initialize from that compact metadata, identify candidate columns, and stream without local staging. The metastore chooses the files. CLP metadata narrows the work inside each archive.

At Uber, the roughly 25 PB of raw logs produced each month compress to 750–850 TB, while five 48-core workers serve CLP data from object storage. The connector presents both metadata layers through an ordinary Presto catalog.

## Deploying and Querying with the CLP Connector

From a Presto user's perspective, the current open-source connector exposes storage-native scans through ordinary SQL. One query can combine exact typed paths, cross-field search, and complete-event reconstruction:

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

Replace `application_logs` and the paths with a deployed dataset. Typed accessors state the expected runtime type. The wildcard-key predicate searches string fields without naming an exact key. Full JSON is reconstructed only for projected matches. An optional Presto table schema, called a **convenience schema**, can expose frequently used CLP paths as ordinary typed columns. These accessors bridge today's PrestoDB type system. Once Presto supports Variant operators end to end, the connector can expose the same paths through standard Variant syntax. That changes the SQL surface, not CLP's path binding or physical scan. YScope's [August 2025 walkthrough](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) covers rewriting and pushdown.

For an operator, the deployment path is short: ingest with CLP, install the matched Java coordinator plugin and native worker bundle, enable the worker loader, and create `etc/catalog/clp.properties`. The [documented setup](https://docs.yscope.com/presto/connector/clp.html) reads MySQL metadata populated by the YScope CLP OSS package. Custom providers can replace it. The connector is read-only and exposes discovered datasets in its `default` schema. Its [packaging flow](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/tools/build-packages/README.md) supports OS packages, tarballs, and an init-container image.

Two Compose examples make the deployment concrete. The connector's [coordinator/native-worker stack](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/integration-tests/docker-compose.yaml) installs matched artifacts through the shipped installer image. The CLP [deployment stack](https://github.com/jackluo923/clp/blob/cf9e2fab8e307a90a1009364c7a21d7a7b0a1727/tools/deployment/presto-clp/docker-compose.yaml) places the coordinator and worker in a complete CLP environment.

Behind that short operator path is a two-part coordinator–worker contract.

## Building Pluggable Presto Native Connectors: Lessons from CLP

An out-of-band connector for [Presto Native, documented under Presto C++](https://prestodb.io/docs/current/presto-cpp.html), bridges the Java coordinator and Velox-based workers. YScope engineers collaborated with the Presto community to build CLP as one of the first end-to-end plugins across that boundary. The earlier integration already ran natively, but lived at the Velox execution layer with corresponding extensions in forked Presto/Prestissimo. It was an in-tree integration, not an external plugin. The current connector targets the Presto Native worker-plugin boundary as a separate library built against Presto and Velox headers and loaded out of band. Native execution moved out of the fork, not out of the worker.

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

**Define the coordinator–worker contract.** Start with tables, types, splits, pushdowns, residuals, and cross-boundary handles. Java publishes the connector factory, metadata, split manager, functions or optimizer, and handle codecs. CLP's [`ClpPlugin`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/presto-connector/src/main/java/com/facebook/presto/plugin/clp/ClpPlugin.java) and [`ConnectorCodecProvider`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/presto-connector/src/main/java/com/facebook/presto/plugin/clp/codec/ClpConnectorCodecProvider.java) are examples. The native [`registerExtensions`](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/velox-connector/src/ClpPluginEntry.cpp) entry point registers the Velox connector factory and handle adapter. `plugin.dir` locates the bundle, and a linker version script exposes only that entry point.

Two Presto hooks complete that contract. [PR #27653](https://github.com/prestodb/presto/pull/27653) added factory registration from dynamically loaded code. [PR #27652](https://github.com/prestodb/presto/pull/27652) added connector-specific native deserialization. With `use-connector-provided-serialization-codecs` enabled, workers can discover the native factory and decode handles produced by Java. Execution stays in process, avoiding sidecar RPC and per-record language bridges.

**Package one compatible unit.** CLP builds on `manylinux_2_28` for `amd64` and `arm64`, stages the coordinator JAR and worker `.so`, puts native dependencies under `lib/`, and sets RUNPATH to `$ORIGIN/lib`. That payload feeds packages, tarballs, or an init-container image. Compatibility includes libc, Presto Native, Velox, and CPU flags. Folly's `F14LinkCheck` verifies CPU-intrinsics mode across `dlopen`, so the [CLP build](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/velox-connector/cmake/DeriveTargetCpuFlags.cmake) reuses Velox's `get_cxx_flags` helper.

**Test the deployed boundary.** Query-runner tests cover catalog discovery, split planning, codecs, and handle serialization. Dependency-free native helpers remain independently testable. Registration, deserialization, data sources, and vectors need a real worker because their symbols resolve at runtime. CLP's cumulative [integration-test stack](https://github.com/y-scope/clp-plugin-presto-connector/pull/64) makes `task integration-tests:run` build the installer and launch a Java coordinator and native worker over local and S3 archive fixtures.

For an external connector author, the reusable deliverable is the tested coordinator artifact, native bundle, and serialized protocol together.

## Conclusion: Choosing Semi-Structured Storage for Presto

There is no universal storage model for semi-structured data. The lakehouse Variant stack fits when cross-engine portability matters and writers can identify paths for shredding before writing. CLP fits when the analytical platform cannot control upstream schemas or content, recurring structure and text can be exploited, and users need to search across fields they could not predict at ingestion. Stable schemas and nearly unique shapes reduce CLP's advantages.

The models are complementary rather than interchangeable. Once PrestoDB supports Variant end to end, CLP's typed accessors can give way to standard Variant operators. Parquet would still read `value` or shredded columns while CLP binds paths to ERT columns and encoded text streams. The open-source [CLP connector](https://github.com/y-scope/clp-plugin-presto-connector) demonstrates the larger Presto lesson: common SQL and planning can coexist with storage-specific native execution behind a pluggable connector boundary.

## Acknowledgments

At YScope, Jack Luo, Kirk Rodrigues, Rui Wang, Xiaochong Wei, and Chenxing Yang led the connector implementation. From IBM and the Presto community, Pratik Joseph Dabre, Matt Graham, Deepak Majeti, and Tim Meehan contributed infrastructure and review. At Uber, Karthik Bhat, Qingqin Hou, Curt Hu, Maksym Ivanchenko, Jianjian Xie, and Yun Zhang worked with YScope engineers to extend and apply the architecture in production. Special thanks to everyone who helped turn dynamic loading and connector-specific serialization into reusable Presto Native infrastructure.
