# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

## Article

- **Goal:** Explain how Presto can query specialized semi-structured storage without absorbing the storage engine into its codebase.
- **Audience:**
  - Presto users and operators evaluating semi-structured data storage.
  - Presto maintainers and developers building out-of-band Presto Native connectors.
  - Storage and platform engineers comparing standardized Variant tables with specialized formats such as CLP.
- **Why:** Variant is becoming the standard lakehouse reference for semi-structured data, while Presto Native is opening an external connector model. Readers need both developments to understand where CLP fits.
- **Takeaway:** The pluggable connector preserves CLP's native columnar model behind a reusable Presto Native extension boundary. Variant is both essential background and complementary at the SQL layer; Iceberg/Parquet Variant and CLP remain different physical choices for different workloads.
- **Important points:**
  - Primary subject: the current open-source CLP connector for Presto Native.
  - Supporting comparison: Iceberg Variant and Parquet Variant shredding.
  - Additional value: practical guidance for operators and external connector authors.
  - Prior-post boundary: the August 2025 YScope article owns the original motivation, core CLP concepts, accessor and optimizer mechanics, split discovery, pushdown, and conventional-Parquet evaluation. This article repeats only the minimum needed to support the new Variant comparison, current-connector architecture, or operator guidance.

## Opening: A common SQL type is not a storage design

- **Goal:** Establish the semi-structured data problem, connect to the earlier YScope article, and introduce the current connector.
- **Why:** The earlier motivation and implementation history appear only as a short baseline. Readers need that baseline to understand what the Variant comparison changes and what moved outside the fork.
- **Takeaway:** Presto standardizes access to data, not its physical organization.
- **Important points:**
  - Evolving logs and similar records defeat fixed schemas; JSON preserves them but loses field-level columnar execution.
  - YScope's [August 2025 post](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82): CLP model, SQL access, pushdown, and native integration in a Presto fork.
  - Historical boundary: native Velox execution, but changes required inside the Presto fork and Velox.
  - Current boundary: independently developed shared library loaded by Presto Native.
  - PrestoDB lacks end-to-end Variant support; Variant is context and a physical-storage comparison, not a currently deployable alternative.

## 1. Variant establishes a multi-layer lakehouse baseline

- **Goal:** Teach Variant as a stack of related SQL, table, and file-format capabilities, then locate CLP at the correct layer of comparison.
- **Why:** Variant is becoming the standard lakehouse model for semi-structured data. Presto readers need to distinguish its layers to understand both its value and why it does not make specialized storage obsolete.
- **Takeaway:** A Variant SQL type can complement CLP as a common logical surface. Iceberg/Parquet Variant supplies a standardized storage alternative, while CLP preserves a different native columnar layout and search model.
- **Important points:**
  - Three layers: dynamic SQL type and operators; Iceberg v3 table type; Parquet `metadata`/`value` encoding and optional shredding.
  - Unshredded Variant: row-local binary value; known-path navigation without textual JSON parsing, but no independent inner-field columns or statistics.
  - Shredding: selected paths gain typed columns, compression, projection, statistics, and skipping.
  - Physical choice made before file finalization; newly promoted paths require file rewrites.
  - Complementarity by layer:
    - Future PrestoDB Variant SQL could expose an `event VARIANT` backed by CLP.
    - Iceberg/Parquet Variant and CLP remain alternative physical layouts; neither must replace the other.
  - Best fit: interoperable lakehouse tables with predictable hot paths.
  - Trino: useful precedent in the Presto lineage, not a drop-in PrestoDB feature.

## 2. What the connector preserves from CLP

- **Goal:** Explain why CLP is more than another logical representation for dynamic values.
- **Why:** Readers need enough CLP background to understand what the connector preserves, but YScope's [August 2025 post](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) provides the full storage-model and original-integration explanation. This section supplies only the concepts required for the new Variant comparison.
- **Takeaway:** CLP is a semi-structured native columnar format: all observed fields are columnized, and recurring structure plus log-aware encoding improve locality and compression.
- **Important points:**
  - Scope: brief refresher on ERTs, the archive-local schema tree, and logtypes; link to the earlier post rather than repeat its optimizer, pushdown, or benchmark walkthrough.
  - ERTs: recurring key-and-type shapes grouped within an immutable archive.
  - Archive-local schema tree: every observed field and type mapped to encoded columns; polymorphic paths routed to separate homogeneous columns.
  - Execution metadata: exact paths and typed cross-field searches resolved once per archive split.
  - Logtypes: recurring free text separated from typed variables for compressed search below the field boundary.
  - Best fit: uncontrolled recurring schemas, unpredictable paths or types, cross-field searches, and repetitive text.
  - Limits: no field-columnization advantage for stable typed schemas; declining ERT locality for near-unique shapes.
  - Evidence boundary: earlier conventional-Parquet benchmarks do not compare CLP with Variant or Variant shredding.

## 3. How predicates bind to physical storage

- **Goal:** Turn the format comparison into concrete query behavior without repeating the earlier optimizer walkthrough.
- **Why:** Readers need to see how physical layout changes the work behind exact, polymorphic, and cross-field predicates.
- **Takeaway:** The same logical predicate can require different physical work after Presto pushes it down.
- **Important points:**
  - Exact path: shredded Variant column or traversal of `value`; CLP path/type resolution followed by matching ERT columns.
  - Polymorphic path: selected Variant type in `typed_value`, remaining types in `value`; separate homogeneous CLP columns.
  - Typed cross-field search: relevant per-row Variant values unless separately indexed; CLP archive metadata first identifies candidate paths.
  - Current CLP: typed wildcard value search.
  - Future CLP: wildcard key discovery and general `{path, value}` projection.
  - Future PrestoDB Variant: possible common `event VARIANT` over CLP without changing CLP's physical scan.

## 4. Native execution becomes independently pluggable

- **Goal:** Explain the historical-to-current integration change and the reusable Presto Native boundary.
- **Why:** The earlier post establishes the native integration inside a fork. Revisiting it here is necessary only to contrast that boundary with the new external connector model—the article's main Presto contribution.
- **Takeaway:** Native execution moved out of the Presto fork, not out of the worker.
- **Important points:**
  - Java coordinator: catalogs, metadata, splits, rewriting, and handle serialization.
  - Native worker: dynamically load connector `.so`, register factory, deserialize handles, scan CLP, and return columnar batches.
  - In-process benefits: no sidecar service or per-record language bridge.
  - Presto PR #27653: connector-factory registration from dynamically loaded code.
  - Presto PR #27652: connector-specific values routed to native deserialization.
  - Result: external CLP integration without CLP-specific changes embedded in Presto or Velox.

## 5. Operator experience

- **Goal:** Show that the two-part architecture remains straightforward to use.
- **Why:** The earlier post explains how CLP expressions are rewritten and pushed down. This section adds only a current-OSS installation path and one usage example so operators can act on the new connector.
- **Takeaway:** Operators see a conventional Presto catalog backed by matched Java and native artifacts.
- **Important points:**
  - Ingest with CLP; install coordinator and worker artifacts; enable dynamic loading; configure `clp.properties`.
  - One example query: typed exact paths, typed wildcard value search, and whole-event reconstruction.
  - Earlier YScope post: optimizer rewriting and pushdown details.

## 6. Metadata at two scales

- **Goal:** Separate file-level split planning from archive-local query execution.
- **Why:** The earlier post covers metadata-based split discovery. This section revisits metadata to add the new distinction between file-level planning and archive-local execution, then separates current OSS, Uber extensions, and future possibilities.
- **Takeaway:** Coarse metadata decides what to open; archive metadata decides what within it deserves work.
- **Important points:**
  - File level: filter searchable KV-IR files and immutable archives.
  - Uber extensions: billion-file planning and eligible Top-N early termination.
  - Archive level: schema tree, dictionaries, logtypes, and indexes bind paths to encoded columns during object-storage streaming.
  - Future: structural summaries, wildcard key discovery, and semantic candidate narrowing.

## 7. Reusable lessons for connector authors

- **Goal:** Give a developer starting from scratch a compact implementation and release model.
- **Why:** The connector's community value extends beyond CLP if other teams can reuse its coordinator/worker, packaging, and testing patterns.
- **Takeaway:** An external connector is a versioned coordinator/worker contract and tested release bundle—not merely one native library.
- **Important points:**
  - Contract first: tables, splits, pushdowns, residuals, and serialized handles.
  - Two implementations: Java planning/serialization and native registration/scan.
  - One release: Java plugin, `.so`, dependent libraries, RPATH, and compatible Presto/Velox versions.
  - Build matrix: libc, architecture, CPU flags, and Folly `F14LinkCheck` mode.
  - Delivery options: OS package, tarball, or Kubernetes init container.
  - Testing: Java query runner, independent native helpers where possible, and a real native worker for plugin-bound behavior.

## 8. One Presto, different storage contracts

- **Goal:** Close with current capability, production evidence, and the practical storage decision.
- **Why:** Readers should finish with a usable decision rule rather than a claim that either CLP or Variant universally wins.
- **Takeaway:** One SQL engine can support different storage strengths without embedding every storage engine in its codebase.
- **Important points:**
  - Current OSS: KV-IR and archive scans, typed paths, wildcard value search, reconstruction, pushdown, and pluggable metadata.
  - Uber: about 25 PB/month raw compressed to 750–850 TB; five 48-core workers serving object storage; advanced planning remains an Uber extension.
  - Choose Variant when interoperability and standard table semantics dominate.
  - Choose CLP when intrinsic columnization, unpredictable cross-field search, repetitive text, specialized compression, and lower object-storage transfer matter more.
  - Complementary outcome: future Variant SQL and CLP storage can coexist without requiring physical convergence; Iceberg/Parquet Variant remains available for workloads favoring standardized storage.

## Acknowledgments

- YScope engineers: connector implementation.
- IBM engineers and Presto community: infrastructure and review.
- YScope and Uber engineers: production extensions and application.
