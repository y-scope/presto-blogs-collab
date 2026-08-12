# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

## Article

- **Goal:** Show what specialized storage contributes beneath a standard semi-structured SQL interface and how Presto Native exposes it through a pluggable connector.
- **Audience:** Presto users, operators, maintainers, and external connector authors.
- **Why:** Variant is standardizing interchange, but physical layout still determines compression and search behavior at scale.
- **Takeaway:**
  - Variant favors interoperability and predictable paths selected for shredding.
  - CLP exploits recurring document structure and text when producers and query paths cannot be controlled.
  - Presto can retain both common SQL and storage-specific execution through pluggable native connectors.

## Opening: a common SQL type is not a storage design

- **Goal:** Establish the overlooked physical layer and its production stakes.
- **Why:** A standard value type cannot choose the best layout for every workload.
- **Takeaway:** Variant standardizes interchange. Storage still determines compression and search.
- **Important points:**
  - Analytical platforms rarely control evolving upstream producers or know future query paths.
  - Uber's onboarded workloads produce about 25 PB of raw logs per month.
  - Introduce the open-source [CLP project](https://github.com/y-scope/clp) from [YScope](https://yscope.com) and the [OSDI '24 μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui).
  - YScope's [August 2025 post](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) owns the original walkthrough. Its integration ran natively in a Presto fork, not as a plugin.
  - Current availability: CLP works through a pluggable Presto Native connector. Variant does not yet work end to end in PrestoDB.

## 1. Variant in the lakehouse landscape

- **Goal:** Separate Variant's SQL, table, and file layers.
- **Why:** Readers need the standard baseline before comparing CLP.
- **Takeaway:** Variant standardizes a dynamic value. Parquet determines its physical scan.
- **Important points:**
  - **Layers:** Trino as engine precedent, Iceberg v3 as table type, and Parquet as physical encoding.
  - **Unshredded:** Typed row-local value without independent inner-field columns or statistics.
  - **Shredded:** Selected paths gain typed columns, compression, and skipping.
  - Shredding choices are set before opening each file for writing. Later changes require rewrites.
  - Exhaustive shredding creates a wide, sparse physical union.

## 2. CLP: Native Columnar Semi-Structured Storage

- **Goal:** Reveal CLP's two levels of native structurization.
- **Why:** The August 2025 YScope post already covers the full storage and optimizer design.
- **Takeaway:** CLP columnizes document structure and also decomposes recurring text into templates and typed values.
- **Important points:**
  - **Physical analogy:** One Parquet table per file versus many shape-specific CLP subtables behind one logical event stream.
  - **Encoded Record Tables (ERTs):** Defined independently per archive. Internal storage structures, not SQL tables.
  - **Merged Schema Tree (MST):** Merges archive-local ERT schemas and maps paths and types to columns.
  - **Sparsity:** Variant keeps one file-wide schema. ERTs omit absent fields but preserve explicit nulls.
  - **Log templates (CLP logtypes):** Inferred, format-string-like templates encode recurring static text once. Extracted variable values are stored in separate typed ERT columns.
  - **Limits:** Stable schemas or nearly unique shapes reduce CLP's advantage.

## 3. From predicate to physical scan: Parquet Variant and CLP

- **Goal:** Compare reader work after pushdown without repeating optimizer mechanics.
- **Why:** The same SQL predicate can drive different physical scans.
- **Takeaway:** Parquet reads shredded columns or `value`. CLP resolves ERT columns once per archive split.
- **Important points:**
  - **Exact path:** Parquet uses a shredded column when available. CLP resolves matching ERT columns.
  - **Polymorphic path:** Parquet keeps non-declared types in `value`. CLP routes each type to homogeneous columns.
  - **Cross-field value:** Parquet inspects row-local `value` unless indexed. CLP uses archive metadata to select columns.
  - **Current CLP:** Typed value search over wildcard keys.

## 4. Beyond File Pruning: Leveraging Metastore and CLP Metadata

- **Goal:** Separate cross-file metadata from metadata inside an archive.
- **Why:** Each layer eliminates different work.
- **Takeaway:** Two metadata layers do more than prune splits.
- **Important points:**
  - **General planning:** Time bounds, dimensions, and statistics. Uber applies it across billions of file records.
  - **Pruning:** Removes an individual file only when it cannot match. Independent of ordering and limits.
  - **Conventional Top-N:** Limits output, not scan scope. It cannot tell Hive-style split discovery when enough data has been found.
  - **Proof-based Top-N:** Stops enumerating remaining older files once they cannot change the newest `N`, even if they may match.
  - Uses time bounds, supported-predicate counts, and an overlap-aware cutoff. Older files never become splits or reach workers.
  - Refer to Uber's Part III article for the full proof.
  - Time sorting and append-only publication help but do not define the optimization.
  - **Archive metadata:** The MST and log-template/value dictionaries in the header support streaming without local staging.
  - **Production evidence:** About 25 PB/month raw compresses to 750–850 TB. Five workers serve object-storage queries. Early termination is an Uber extension.

## 5. Deploying and Querying with the CLP Connector

- **Goal:** Show the SQL outcome first, then the shortest operator path to it.
- **Why:** Readers should see the outcome before the implementation boundary.
- **Takeaway:** Ordinary Presto SQL drives a storage-native scan.
- **Important points:**
  - Example query: typed paths, wildcard-key search, and whole-event reconstruction.
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
  - **Packaging:** Ship the JAR, `.so`, dependencies, RPATH, and serialized protocol together. Match libc, architecture, Presto/Velox, CPU flags, and Folly mode.
  - Test Java planning with query runners. Use a real worker for runtime-resolved native code.

## 7. Conclusion: Choosing Semi-Structured Storage for Presto

- **Goal:** Convert the comparison into a workload decision.
- **Why:** Readers need conditions, not a universal winner.
- **Takeaway:** Variant and CLP are complementary. Presto can unify access without flattening their physical differences.
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
