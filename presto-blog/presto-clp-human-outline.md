# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

## Article

- **Goal:** Explain what CLP contributes beneath Presto SQL and how its pluggable Presto Native connector exposes those capabilities.
- **Audience:** Presto users, operators, maintainers, and external connector authors.
- **Why:** Common SQL does not erase physical storage choices for semi-structured data.
- **Takeaway:**
  - CLP uses recurring structure and text to improve compression and search when upstream schemas cannot be controlled.
  - Presto exposes those capabilities through a pluggable native connector while retaining common SQL and planning.
  - Variant provides a useful lakehouse baseline, not the article's primary subject.

## Opening: common SQL does not erase storage design

- **Goal:** Establish the physical storage problem and its production stakes.
- **Why:** Evolving applications define data that the analytical platform does not control.
- **Takeaway:** CLP provides specialized storage behind Presto's common SQL interface.
- **Important points:**
  - Fields, types, nesting, and text evolve without coordinated table migrations.
  - Uber serves tens of petabytes of raw logs per month through CLP, with substantial headroom in the current deployment.
  - Introduce [CLP](https://github.com/y-scope/clp), [YScope](https://yscope.com), and the [OSDI '24 μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui).
  - YScope's [August 2025 blog](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) owns the original walkthrough. That integration ran natively in a Presto fork, not as a plugin.
  - Today CLP uses a pluggable Presto Native connector. Variant is introduced only as a brief reference and is not yet supported end to end in PrestoDB.

## 1. Variant in the lakehouse landscape

- **Goal:** Establish the minimum lakehouse baseline needed to understand CLP.
- **Why:** Variant separates the logical value from its table and file representation.
- **Takeaway:** Variant standardizes a dynamic value; optional Parquet shredding accelerates paths selected before writing.
- **Important points:**
  - **Layers:** Engine operators, Iceberg v3 table type, and Parquet physical encoding.
  - **Unshredded:** One native binary value per row.
  - **Shredded:** Selected paths gain typed columns, compression, statistics, and skipping. Adding paths later requires rewriting the file.

## 2. CLP: Native Columnar Semi-Structured Storage

- **Goal:** Explain CLP's structure-aware columnization and text encoding.
- **Why:** The August 2025 YScope blog owns the full storage and optimizer walkthrough.
- **Takeaway:** CLP columnizes document structure and decomposes recurring text into templates and typed values.
- **Important points:**
  - **Physical analogy:** Variant begins with row-local values and selected shredded columns. A CLP archive contains many shape-specific columnar subtables.
  - **ERTs:** Archive-local storage structures that group one key-and-type shape into homogeneous columns.
  - **MST:** Maps the archive's observed paths and types to ERT columns.
  - **Structural absence:** An ERT contains only fields present in its shape; explicit nulls remain preserved.
  - **Log templates:** Encode recurring static text once and store extracted variables in typed ERT columns.
  - **Limits:** Stable schemas, nearly unique shapes, or nearly unique text leave less repetition to exploit.

## 3. From predicate to physical scan: Parquet Variant and CLP

- **Goal:** Compare reader work after pushdown without repeating optimizer mechanics.
- **Why:** The same SQL predicate can drive different physical scans.
- **Takeaway:** The storage layout determines which row values or encoded columns a reader must inspect.
- **Important points:**
  - **Exact path:** Use a shredded Parquet column or resolve matching CLP ERT columns.
  - **Multiple types:** Parquet retains non-declared types in `value`; CLP routes each type to homogeneous columns.
  - **Cross-field typed value search:** Current CLP behavior. The MST selects compatible typed columns without an exact field name.
  - **Wildcard key/path matching:** Distinct future connector work. The MST can identify candidate paths before event scans.

## 4. Beyond File Pruning: Leveraging Metastore and CLP Metadata

- **Goal:** Separate cross-file metadata from metadata inside an archive.
- **Why:** Each layer eliminates different work.
- **Takeaway:** Two metadata layers do more than prune splits.
- **Important points:**
  - **General planning:** Time bounds, dimensions, and statistics. Uber applies it across billions of file records.
  - **Pruning:** Removes an individual file only when it cannot match. Independent of ordering and limits.
  - **Conventional Top-N:** Limits output, not scan scope. It cannot tell Hive-style split discovery when enough data has been found.
  - **Top-N file skipping:** Stops enumerating older files once they cannot change the latest `N`, even if they contain matches.
  - Uses exact matching-event counts and a timestamp watermark. Every overlapping file remains included.
  - Row-level filters that prevent an exact count keep all files remaining after normal pruning.
  - Accurate time bounds and counts are required. Time ordering and append-only publication make the optimization more effective but are not sufficient by themselves.
  - Keep the correctness explanation self-contained because Uber's Part III article will publish later.
  - **Archive metadata:** The MST and log-template/value dictionaries in the header support streaming without local staging.
  - **Production evidence:** Tens of petabytes of raw logs per month with substantial headroom, billions of file records, a roughly 30× raw-to-compressed ratio on enriched logs, and a handful of workers. Treat these as deployment observations, not a format benchmark. Early termination is an Uber extension.

## 5. Deploying and Querying with the CLP Connector

- **Goal:** Show the SQL outcome first, then the shortest operator path to it.
- **Why:** Readers should see the outcome before the implementation boundary.
- **Takeaway:** Ordinary Presto SQL drives a storage-native scan.
- **Important points:**
  - Example query: typed paths, cross-field typed value search, and whole-event reconstruction.
  - Future SQL surface: native Variant operators can replace CLP accessor syntax without changing CLP storage or its physical scan.
  - Optional convenience schema: common CLP paths appear as ordinary typed columns without rewriting archives.
  - Setup: ingest with CLP, install matched artifacts, enable the loader, and configure `clp.properties`.
  - Current boundary: MySQL-backed default provider and read-only access through the `default` schema.
  - Compose examples: matched-artifact installation and complete deployment topology.

## 6. Building Pluggable Presto Native Connectors: Lessons from CLP

- **Goal:** Explain the extension boundary and give connector authors a starting point.
- **Why:** This is the article's main Presto contribution.
- **Takeaway:** Native execution moved out of the fork, not out of the worker.
- **Important points:**
  - YScope engineers collaborated with the Presto community to build one of the first end-to-end, out-of-band connector plugins for Presto Native.
  - **Boundary:** Earlier integration lived in Velox and forked Presto/Prestissimo. The current library loads through the Presto Native worker-plugin interface.
  - **Java:** Catalogs, metadata, splits, rewriting, and handle codecs.
  - **Native:** Load the `.so`, deserialize handles, scan CLP, and return columnar batches.
  - **Presto hooks:** PR #27653 for dynamic factories. PR #27652 for native deserialization.
  - Frame the reusable guidance as three lessons from CLP: define the Java/native contract, package one compatible release unit, and test the deployed boundary.
  - **Packaging:** Ship the JAR, `.so`, dependencies, RPATH, and serialized protocol together. Match libc, architecture, Presto/Velox, CPU flags, and Folly mode.
  - Test Java planning with query runners. Use a real worker for runtime-resolved native code.

## 7. Conclusion: Choosing Semi-Structured Storage for Presto

- **Goal:** Convert the comparison into a workload decision.
- **Why:** Readers need conditions, not a universal winner.
- **Takeaway:** Choose storage for the workload; Presto's pluggable connector preserves CLP's native execution behind common SQL.
- **Important points:**
  - **Choose Variant:** Cross-engine portability with predictable paths for shredding.
  - **Choose CLP:** Uncontrolled schemas, recurring structure/text, and cross-field or free-text search.
  - Stable schemas or nearly unique shapes reduce CLP's advantage.
  - Once PrestoDB supports Variant end to end, CLP accessors can switch to standard Variant operators; Parquet and CLP will still perform different physical scans.
  - The connector demonstrates common planning with storage-specific native execution.

## Acknowledgments

- **YScope:** Jack Luo, Kirk Rodrigues, Rui Wang, Xiaochong Wei, and Chenxing Yang. Connector implementation.
- **IBM/Presto:** Pratik Joseph Dabre, Matt Graham, Deepak Majeti, and Tim Meehan. Infrastructure and review.
- **Uber:** Karthik Bhat, Qingqin Hou, Curt Hu, Maksym Ivanchenko, Jianjian Xie, and Yun Zhang. Production extensions and application with YScope.
