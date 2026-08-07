# Bringing Schema-Less Data Analytics to Presto Native with CLP

Presto's connector model begins with a reassuring contract: before a query runs, the catalog can tell the planner which columns and types exist. Schema-less data breaks that contract. Applications change fields without coordinating with Presto operators; the same path may hold several types; nested arrays may mix strings, numbers, objects, and arrays. A user may even know the value they need without knowing its key. Logs are a demanding example, but not the boundary of the problem.

Putting every event in one JSON string preserves the bytes, but postpones the problem. SQL cannot naturally project fields unknown during planning or search every value without enumerating columns, and repeated document parsing can forfeit storage-level filtering and projection.

The [Compressed Log Processor (CLP)](https://github.com/y-scope/clp) instead preserves each archive's evolving structure in a compact, directly searchable representation. Common fields can remain ordinary columns without defining the limits of the data. Integrating this model required a practical external connector path for Presto Native and a SQL surface where schema-aware and schema-less operations coexist. The open-source result is usable today and underpins Uber's production deployment.

## CLP in Two Minutes

CLP losslessly compresses and searches evolving data, from natural-language messages with embedded variables to JSON events, including operations logs and coding-agent traces.

The boundary between “unstructured” and “structured” logs is increasingly blurry. A natural-language message may sit inside a JSON envelope enriched with correlation, platform, and host identifiers, while a request-oriented JSON event may contain essential prose. JSON describes the encoding, not the stability of its schema. CLP handles this full spectrum.

Consider two messages:

~~~text
Request 123 failed after 45 ms
Request 987 failed after 12 ms
~~~

CLP stores their recurring text once:

~~~text
Request <*> failed after <*> ms
~~~

The changing values are typed and encoded separately while the messages remain losslessly reconstructable.

Semi-structured logs also repeat field names, nesting, and structural patterns. CLP-S stores that structure once per archive rather than repeating every key in every event. Later splits can introduce fields or types without changing a dataset-wide contract. The model is recursively polymorphic: object values may vary by event, and arrays may mix scalars, objects, and other arrays without coercion.

CLP-S uses schema information, logtype dictionaries, typed values, and statistics to find matches and reconstruct selected values without expanding every event into raw JSON.

The resulting footprint can be substantially smaller than a general-purpose columnar representation. In a controlled comparison over the same losslessly represented log corpus, CLP-S produced **[CLP-S SIZE]**, compared with **[PARQUET SIZE]** for Parquet using **[CODEC AND SETTINGS]**—a **[X]× smaller compressed footprint**.

The compression and search design is described in [μSlope: High Compression and Fast Search on Semi-Structured Logs](https://www.usenix.org/conference/osdi24/presentation/wang-rui), an OSDI '24 collaboration among YScope, the University of Toronto, and Uber. CLP-S is written in C++—an important property for this integration.

## Familiar Architecture, Different Data Contract

The connector follows a familiar Presto pattern.

| Hive or Iceberg concept | CLP analogue |
|---|---|
| Catalog or metastore | Pluggable CLP metadata provider |
| Registered table schema | Optional global view of common discovered fields, plus schema-less access |
| Parquet or ORC file | CLP-S archive |
| File or row-group split | CLP archive split |
| Parquet or ORC reader | Native CLP-S search and projection |
| Partition or manifest pruning | Metadata-level archive pruning |

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
        metadata["Pluggable metadata provider<br/>Archives · statistics · optional global field view"]
        archives["CLP-S archives<br/>Compressed events · split-local structure"]
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

**Figure 1: The CLP connector spans Presto's Java coordinator and native C++ workers. Metadata guides planning, while workers load CLP-S and search the selected archives.**

The boxes are familiar; the contract is not. Hive and Iceberg normally begin with a catalog schema. CLP records structure within each split and requires no schema before ingestion or querying. A deployment may aggregate those structures into an optional global view of common Presto columns, but schema-less functions can still resolve unexposed fields and types against each split. The connector never needs to union every field observed across all splits into one mandatory table schema. One query can therefore combine ordinary filters, dynamic access, and relational aggregation.

## A Native Engine Inside a Native Engine

CLP-S and Presto Native both execute their performance-critical paths in C++. Unlike a network-bound remote connector, CLP performs a specialized storage scan in which schema navigation, compressed search, and selective decoding are the hot path.

The connector loads CLP-S into each Presto-native worker as a shared library. A worker receives a split, streams the archive, evaluates pushed-down operations through the native reader, and produces values for Presto. This avoids a sidecar, JNI scan path, extra RPC hop, and raw-JSON materialization.

This required more than loading a library. The plugin must register a connector factory, while the Java coordinator must create connector-specific splits and handles, serialize them into the plan, and route them to the correct C++ deserializer.

Building the CLP connector exposed gaps in that path. YScope engineers contributed two important upstream changes:

- [PR #27653](https://github.com/prestodb/presto/pull/27653) hoisted native plugin loading and made connector-factory registration available to dynamically loaded code.
- [PR #27652](https://github.com/prestodb/presto/pull/27652) added routing for connector-specific serialized values used by native deserialization.

Together, these changes completed important pieces of the out-of-band connector path, with CLP as an early real-world reference implementation.

That boundary matters beyond CLP. A specialized engine can retain its native dependencies and release lifecycle while participating in Presto's catalog, planning, split, and execution model. The connector becomes an extension rather than a permanent addition to the Presto monorepo.

YScope engineers were the primary connector contributors. IBM engineers and the broader Presto community contributed infrastructure, architectural support, and review. YScope engineers then worked alongside Uber engineers to apply and extend the architecture in production, combining Uber's workloads and platform integration with joint metadata and query-planning engineering.

CLP was not rewritten for Presto Native; Presto Native gained the extension path to load CLP's C++ engine as an external connector.

## SQL Where the Schema Ends

Native execution solves only half the problem. Users still need to express schema-less operations in SQL. Common fields such as timestamp, service, severity, and host remain ordinary columns:

~~~sql
SELECT service, count(*) AS errors
FROM clp.logs
WHERE timestamp >= current_timestamp - INTERVAL '1' HOUR
  AND severity = 'ERROR'
GROUP BY service
ORDER BY errors DESC;
~~~

This works with existing query builders, dashboards, joins, and aggregation. But no catalog view can anticipate every field an investigation may require. Relational SQL cannot naturally:

- Search values without knowing their keys.
- Address unexposed paths or select one type of a polymorphic path.
- Represent a heterogeneous nested array through `ARRAY(T)`.
- Return an event whose structure is absent from the planned schema.

SQL `LIKE` can match a known string column, not field names absent from the relation. [Presto 0.298.1](https://prestodb.io/docs/current/language/types.html) has `JSON` but no first-class `VARIANT`. JSON requires extraction and casting for typed use; `ROW` has a fixed shape and `ARRAY(T)` one element type. None provides a typed result that preserves a path's runtime type, including heterogeneous array elements. Generic JSON functions also do not automatically preserve CLP-S pushdown.

The [public connector exposes](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/presto-connector/src/main/java/com/facebook/presto/plugin/clp/ClpFunctions.java) typed accessors including `CLP_GET_BIGINT`, `CLP_GET_DOUBLE`, `CLP_GET_BOOL`, `CLP_GET_STRING`, and `CLP_GET_STRING_ARRAY`. They give the planner a stable type while CLP resolves the path per split. Typed wildcard functions search values across fields, and `CLP_GET_JSON_STRING()` returns the complete event without constructing a dynamic Presto `ROW`.

This makes the current boundary explicit. Typed accessors are the efficient interface when users know the type they want. Complete-event JSON is the type-agnostic escape hatch when they do not know the full shape. Schema-aware columns and schema-less access are complementary, not competing modes.

The following query uses the public connector API:

~~~sql
SELECT
    timestamp,
    service,
    CLP_GET_STRING('request.id') AS request_id,
    CLP_GET_BIGINT('response.status') AS status,
    CLP_GET_JSON_STRING() AS event_json
FROM clp.logs
WHERE timestamp >= current_timestamp - INTERVAL '1' HOUR
  AND service = 'checkout'
  AND CLP_GET_BIGINT('response.status') >= 500
  AND CLP_WILDCARD_STRING_COLUMN() LIKE 'timeout%'
ORDER BY timestamp DESC
LIMIT 100;
~~~

Here, ordinary columns provide coarse filters; `CLP_GET_BIGINT` selects `BIGINT` values from a polymorphic path; `CLP_WILDCARD_STRING_COLUMN` searches strings across fields; and `CLP_GET_JSON_STRING()` reconstructs each match. Presto supplies Top-N.

These are not record-at-a-time JSON wrappers. Their Java bodies are planner placeholders. The [connector rewrites](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/presto-connector/src/main/java/com/facebook/presto/plugin/clp/optimization/ClpUdfRewriter.java) `CLP_GET_*` calls into requested scan fields and translates compatible filters into CLP operations. The function supplies the SQL surface, planning carries its intent, CLP-S executes pushed-down work, and Presto evaluates residual expressions.

A future Presto `VARIANT` could provide type-agnostic access to a **known** path, including nested objects and heterogeneous arrays, while typed functions remain useful for pushdown. It would not find unknown keys: Dremio's [`VARIANT_GET`](https://docs.dremio.com/dremio-cloud/sql/sql-functions/functions/VARIANT_GET/), for example, requires an explicit path and does not accept wildcard paths. CLP's schema tree points toward the complementary operation—matching field patterns and returning both paths and differently typed values. General wildcard-field projection remains a future direction.

In CLP, the schema is a convenience and an optimization—not a boundary around the data.

## Avoid Work, Then Search the Rest in Compressed Form

At log scale, CLP combines work avoidance with fast execution of what remains.

First, metadata planning avoids archives. The open-source connector defines pluggable interfaces for metadata and split filtering, with a basic MySQL-backed implementation; Uber and YScope use more advanced internal metastores for larger deployments. Time bounds, indexed dimensions, common-field views, and statistics let the connector produce only a safe candidate split set. The structure needed to search an archive remains intrinsic to that archive.

Second, CLP-S uses structural metadata, logtypes, typed encodings, and statistics to search each surviving archive. It reconstructs only matching events and projections instead of every raw JSON document.

Search is streaming, but it is not equivalent to decompressing and scanning the raw stream. CLP-S searches the encoded representation and reconstructs raw values only in regions likely to contain matches. Compression therefore reduces both transferred bytes and execution work.

The stages solve different problems:

- Metadata determines what does not need to be read.
- CLP-S makes the data that remains inexpensive to search.

Uber's production deployment adds planning-time early termination for eligible queries shaped like:

~~~sql
ORDER BY timestamp DESC
LIMIT N
~~~

This is not a generic `ORDER BY ... LIMIT` optimization. It requires append-only logs, a finite limit, newest-first timestamp order, time-ordered archives with trustworthy bounds, and proof that unseen older archives cannot change the result. After applying top-level filters, metadata streams candidates in time order and stops producing splits once that proof holds. Presto may still execute selected splits in any order. Pruning removes archives that cannot match; early termination removes older candidates that cannot affect Top-N. The optimizations are orthogonal.

Uber workers search CLP-S archives directly from object storage without local caching or disk staging, beginning as bytes arrive. API limits hold queries to the minimum bandwidth needed for a responsive experience, preserving shared capacity. Maximum benchmark throughput is unnecessary; queries already meet user expectations.

## From Shared Library to Running Catalog

The integration also provides a five-part playbook for external native connectors.

### 1. Define the connector contract

Define tables, types, splits, pushdowns, and returned values—including any schema-less operations.

### 2. Divide coordinator and worker responsibilities

The coordinator handles discovery, planning, and handle serialization. Native code registers the factory, deserializes handles, scans, and produces vectors. Treat this Java–C++ boundary like a protocol: both sides can build while their representations drift.

### 3. Build and package both halves

Build coordinator and native artifacts against compatible Presto Native and Velox versions. The public repository packages `libclp-plugin-velox-connector.so` for `.deb`, `.rpm`, and `.tar.gz` distribution. It targets manylinux_2_28, bundles transitive libraries, and adjusts RPATH for portability. Test the complete coordinator-to-worker path.

### 4. Deploy and register the catalog

1. Install the coordinator-side component.
2. Install or mount the native library on every worker.
3. Configure Presto Native to load the plugin.
4. Add the catalog properties.
5. Configure metadata and split providers.
6. Start the cluster and verify discovery.

The public CLP connector uses catalog properties such as:

~~~properties
connector.name=clp
clp.metadata-provider-type=mysql
clp.split-provider-type=mysql
clp.split-filter-provider-type=mysql
~~~

The [main CLP project](https://github.com/y-scope/clp) provides minimal Kubernetes and Docker Compose setups. The [connector documentation](https://docs.yscope.com/presto/connector/clp.html) covers metadata, split filtering, types, and configuration.

### 5. Validate the boundary

Test `SHOW TABLES`, `DESCRIBE`, ordinary and schema-less queries, pushdown, missing fields, and coordinator/worker compatibility. In particular, test connector-handle serialization across the Java/C++ boundary; independent unit tests on each side will not catch a wire-format mismatch. Out-of-band loading then lets a domain team own its repository, dependencies, and release lifecycle outside the Presto monorepo.

## Open Source Today, Production Validated, Built for More

The [open-source connector](https://github.com/y-scope/clp-plugin-presto-connector), integrated into [CLP](https://github.com/y-scope/clp), is usable end to end today. It supports dynamic field exposure, schema-less rewriting, native pushdown, polymorphic typed access, wildcard value searches, complete-event projection, and pluggable metadata filtering.

YScope and Uber extended the architecture in production. Capabilities not currently in OSS include planning-time early termination, advanced metadata planning, and billion-file metadata scale, alongside Uber's object storage, routing, and operational systems.

The onboarded workload is approximately 25 PB of raw logs per month, compressed to 750–850 TB depending on the mix—a 29–33× reduction. Five 48-core Presto-native workers serve this warm and cold workload directly from object storage. Not every Uber service is onboarded, but the figures show how compression, work avoidance, and native execution can support a large logical workload with a small query cluster.

Uber and YScope intend to contribute additional generally useful components, although scope and timing remain open.

Compression metadata also points beyond scans. CLP-S records structures, types, log patterns, frequencies, and statistics. Today they support discovery, pruning, and encoded search. Tomorrow, analysis could operate first on deduplicated patterns and compact structural metadata—rather than every repeated raw event—to summarize datasets, detect structural changes, or narrow candidates for semantic analysis. Compression can help the system understand what exists before deciding what to read.

## Conclusion

The CLP connector expands Presto in two directions. It gives external C++ storage engines a practical native connector path: CLP-S owns the compressed scan while Presto owns planning, distributed execution, and relational composition. It also expands what SQL can express over evolving data: common fields remain familiar, schema-less functions reach beyond them, and pushdown preserves storage-native execution.

The open-source integration is available today as both a schema-less query system—particularly well suited to logs—and a reference for connector authors.

CLP expands both what Presto can connect to and what a Presto query can express.
