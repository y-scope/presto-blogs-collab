# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

## Article

- **Goal:** Explain what CLP contributes as specialized storage and how Presto Native loads it without a fork.
- **Audience:** Presto users, operators, maintainers, and external connector authors.
- **Why:** Variant is becoming the lakehouse standard for semi-structured values, while Presto Native now supports out-of-band connectors.
- **Takeaway:** One SQL interface can preserve different physical storage strengths.

## Opening: a common SQL type is not a storage design

- **Goal:** Establish the storage problem and the historical transition.
- **Why:** Analytical platforms rarely control evolving upstream producers.
- **Takeaway:** Standardizing access does not determine compression, layout, or search.
- **Important points:**
  - JSON preserves unknown fields but loses field-level columnar execution. A global schema requires upstream coordination.
  - YScope's [August 2025 post](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) owns the original CLP concepts and implementation walkthrough.
  - The earlier integration ran natively in workers from a Presto fork. The current connector is independently developed and loaded out of band as a shared library. PrestoDB does not yet support Variant end to end.

## 1. Variant in the lakehouse landscape

- **Goal:** Separate Variant's SQL, table, and file layers.
- **Why:** Readers need the standard baseline before comparing CLP.
- **Takeaway:** Variant standardizes a dynamic value. Parquet still determines its physical scan.
- **Important points:**
  - Query engine: SQL type and operators, with Trino as Presto-lineage precedent. Iceberg v3: table type. Parquet: `metadata`/`value` encoding and optional shredding.
  - Unshredded: typed row-local value without independent inner-field columns or statistics.
  - Shredded: writer-selected paths gain typed columns and skipping. Changing the selection requires rewriting files. Exhaustive shredding creates a wide, sparse union.

## 2. CLP's distinctive storage model

- **Goal:** Retain only the CLP concepts required for the Variant comparison.
- **Why:** The August 2025 YScope post already provides the full ERT, schema-tree, logtype, optimizer, and benchmark walkthroughs.
- **Takeaway:** CLP is column-first and uses recurring structure and text as compression and search locality.
- **Important points:**
  - A Parquet file is one table with Variant values and optional shredded columns. A CLP archive contains ERTs whose records share a key-and-type shape in homogeneous columns.
  - The archive-local schema tree maps every observed path and type. ERTs are storage structures, not SQL tables.
  - Comprehensive Variant shredding remains a file-wide sparse schema. ERTs omit fields absent from their shape while preserving explicit source nulls.
  - Logtypes separate recurring static text from variables, improving compression and narrowing encoded text search when patterns recur.
  - Best fit: upstream schemas and useful paths cannot be controlled, with compact archives reducing object-storage transfer and request costs. Stable schemas or nearly unique shapes reduce the advantage. Earlier conventional-Parquet benchmarks do not compare against Variant.

## 3. From predicate to physical scan: Parquet Variant and CLP

- **Goal:** Compare reader work after pushdown without repeating optimizer mechanics.
- **Why:** The same SQL predicate can drive different physical scans.
- **Takeaway:** Parquet reads a shredded column or traverses `value`. CLP resolves compatible ERT columns once per archive split.
- **Important points:**
  - Compare exact paths, polymorphic paths, and cross-field typed value search in one table.
  - Current CLP supports typed value search over wildcard keys.
  - Returning matched keys and general `{path, value}` projection remain future work.

## 4. Beyond file pruning: how metastore planning and CLP archive metadata narrow the search

- **Goal:** Separate metadata across files from metadata inside a selected archive.
- **Why:** Each layer eliminates different work.
- **Takeaway:** The metastore chooses splits. CLP metadata narrows each archive scan.
- **Important points:**
  - General planning uses time bounds, dimensions, and statistics. Uber applies this pattern across billions of file records.
  - Eligible newest-first Top-N queries can terminate safely with trustworthy time bounds, exact supported-predicate counts, and an overlap-aware watermark. Time sorting and append-only publication help but are not required.
  - CLP additionally stores a complete path/type-to-column map plus dictionaries and indexes near the archive front, allowing worker initialization and streaming without local staging.
  - Uber's early termination is a production extension. Structural/logtype summaries and semantic candidate narrowing remain future work.

## 5. From deployment to first query with the CLP connector

- **Goal:** Show the shortest operator path to a working catalog.
- **Why:** Readers should see the outcome before the implementation boundary.
- **Takeaway:** Ordinary Presto SQL drives a storage-native scan.
- **Important points:**
  - Ingest with CLP, install matched coordinator and worker artifacts, enable the loader, and configure `clp.properties`.
  - The documented provider reads MySQL metadata populated by CLP ingestion. The connector is read-only and uses the `default` schema.
  - One query demonstrates typed paths, wildcard-key search, and whole-event reconstruction. Compose examples show the matched-artifact and complete deployment topologies.

## 6. Building a pluggable Presto Native connector: CLP as a reference

- **Goal:** Explain the extension boundary and give connector authors a reusable starting point.
- **Why:** This is the article's main Presto contribution.
- **Takeaway:** Native execution moved out of the fork, not out of the worker. Ship the coordinator artifact, native bundle, and serialized protocol as one tested unit.
- **Important points:**
  - Java owns catalogs, metadata, splits, rewriting, and handle codecs. Native workers load the `.so`, deserialize handles, scan CLP, and return columnar batches.
  - PR #27653 added dynamic factory registration. PR #27652 added connector-specific native deserialization.
  - Define tables, types, splits, pushdowns, residuals, and cross-boundary handles before implementing either half.
  - Package the JAR, `.so`, dependencies, RPATH, and version pins together. Match libc, architecture, Presto/Velox, CPU flags, and Folly `F14LinkCheck` mode.
  - Test Java planning and serialization with query-runner infrastructure. Use a real native worker for registration, deserialization, data sources, and vectors whose symbols resolve at runtime.

## 7. Conclusion: choosing a storage model for semi-structured data in Presto

- **Goal:** Convert the comparison into a workload decision.
- **Why:** Readers need conditions, not a universal winner.
- **Takeaway:** Variant and CLP can share a future SQL interface while retaining different scans.
- **Important points:**
  - Variant: cross-engine portability and predictable paths for shredding.
  - CLP: uncontrolled schemas, intrinsic columnization, recurring shape/text locality, and cross-field or free-text search. Stable or unique shapes reduce the advantage.
  - Current OSS connector capabilities and Uber's production scale provide evidence. Keep Uber's advanced planning separate from current OSS.
  - Future Variant SQL could unify access. Parquet would still read `value` or shredded columns, while CLP would bind paths to ERT columns and encoded streams.

## Acknowledgments

- YScope: connector implementation.
- IBM and Presto community: infrastructure and review.
- YScope and Uber: production extensions and application.
