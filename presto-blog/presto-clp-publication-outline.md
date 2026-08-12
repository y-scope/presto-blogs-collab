# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

- **Status:** Editorial outline synchronized with the current draft
- **Audience:** Presto users, operators, maintainers, and external connector authors
- **Target:** 2,150–2,300 words
- **Primary subject:** The current pluggable CLP connector for Presto Native
- **Supporting background:** Variant as the standardized lakehouse reference for semi-structured data
- **Point of view:** Presto users and maintainers deciding when specialized storage belongs behind a common SQL interface

---

## Editorial Contract

The article explains what specialized storage can contribute beneath a standard semi-structured SQL interface, then shows how Presto Native exposes CLP through a pluggable connector. Variant supplies the broader lakehouse context. It is not a currently available PrestoDB alternative or a co-equal subject.

> CLP's distinctive storage model combines intrinsic columnization, recurring-shape locality, log-specific encoding, and storage-native search. The connector exposes those capabilities behind a reusable Presto Native extension boundary. A future Variant SQL value could complement CLP logically without making Parquet Variant and CLP physically equivalent.

Keep this hierarchy:

1. **CLP storage model:** Use Variant briefly as a familiar lakehouse baseline, then explain CLP's intrinsic columnization, recurring-text encoding, and storage-native search.
2. **Connector evolution:** Show how native execution moved from modifications in a Presto fork and Velox to an out-of-band shared library.
3. **Community value:** Give operators a short usage path and connector authors a compact, reusable bootstrap pattern.

Readers should leave able to decide when portability and predictable hot paths favor Variant, when uncontrolled recurring structures and cross-field or encoded-text search favor CLP, and which capabilities are current OSS, Uber production extensions, or future directions.

---

## Narrative Map and Budget

| Section | Editorial job | Target |
|---|---|---:|
| Opening | Lead with Presto's storage boundary, production stakes, prior blog, and current connector | 240–260 |
| 1. Variant landscape | Establish only the SQL/table/file layers and the shredding decision | 110–130 |
| 2. CLP model | Explain CLP's physical model, benefits, and workload-dependent limits | 260–290 |
| 3. Predicate binding | Use one table to distinguish exact paths, polymorphism, cross-field value search, and future wildcard-key matching | 180–200 |
| 4. Metadata beyond pruning | Use metastore and archive metadata as production evidence | 485–515 |
| 5. Deployment to first query | Show the SQL outcome first, then the short operator path | 235–260 |
| 6. Pluggable connector reference | Explain the Java/native contract and compact reusable author guidance | 460–500 + diagram |
| 7. Conclusion | Give the practical storage choice and connector lesson once | 90–110 |
| Acknowledgments | Credit implementation, infrastructure, review, and production application | 70–90 |

Each section must create the question answered by the next: landscape → storage model → query work → metadata → operator experience → reusable connector architecture → decision.

---

## Prior Foundation

YScope's August 2025 blog, [“Querying CLP-compressed logs with SQL via Presto”](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82), owns the original motivation and the introductions to Encoded Record Tables (ERTs), Merged Schema Trees (MSTs), and logtypes. It also owns the typed-accessor design, UDF rewriting, split discovery, pushdown mechanics, and evaluation against conventional Parquet.

This article may define ERTs, the MST, and logtypes briefly because readers need them to understand CLP's physical model, and may include one compact current-connector query. It must not reproduce the earlier optimizer walkthrough, metadata-query generation, plan diagrams, or benchmarks. Cite the earlier blog at the first CLP–Presto history discussion.

---

## Section Contracts

### Opening: SQL Abstraction Does Not Erase Storage Design

- Lead with Presto's abstraction boundary: common SQL does not make physical storage layouts interchangeable.
- Frame schema variability as an ownership problem: analytical platforms do not control evolving applications or future query paths.
- Define the open-source [CLP project](https://github.com/y-scope/clp) from [YScope](https://yscope.com) as a native columnar format and search engine. Establish operating scale with Uber's tens of petabytes of raw logs per month and substantial deployment headroom, not detailed infrastructure figures or comparative benchmark evidence. Cite the [OSDI '24 μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui) for the storage model.
- Cite YScope's August 2025 blog for the original Presto integration. State the history precisely: it executed CLP natively through Velox in a Presto fork and was not an external plugin.
- Present the current pluggable connector as the article's subject. Introduce Variant only as a brief lakehouse reference that PrestoDB does not yet support end to end.
- End by promising a short Variant baseline followed by CLP's storage model and coordinator–worker integration.

### 1. Variant in the Lakehouse Landscape

- Define only the baseline needed later: engines provide the dynamic SQL type and operators, [Iceberg v3](https://iceberg.apache.org/spec/#variant-type) defines the table type, and Parquet defines the binary [`metadata`/`value` encoding](https://parquet.apache.org/docs/file-format/types/variantencoding/) and optional [shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/).
- Describe unshredded Variant as a row-local native binary value.
- Explain that shredding gives writer-selected paths independent typed columns for compression, projection, statistics, and skipping. Writers choose those paths before opening the file; adding them later requires rewriting it.
- End by handing the question to CLP: what can specialized storage do when useful paths and types cannot be predicted?
- Keep implementation history and extended background reading out of the narrative. Use specifications as links on the concepts they support.

### 2. CLP: Native Columnar Semi-Structured Storage

- Open with a direct reference to YScope's [August 2025 blog](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) for the full explanation. Repeat only the two physical ideas needed here: document structure through ERTs and the MST, and recurring-text decomposition through logtypes and typed value columns.
- Use one brief physical contrast: Parquet Variant begins with row-local values and selected shredded columns, while a CLP archive contains many shape-specific columnar subtables behind one logical event stream. Avoid “nested tables,” which could imply a SQL-visible hierarchy.
- Define an encoded record table (ERT) and the archive-local Merged Schema Tree (MST). Make clear that they are archive-local storage structures, not SQL tables or a global schema.
- Explain that ERTs omit structurally absent fields while preserving explicit source nulls.
- Explain logtypes once and connect recurring structure, text, and values to compression and narrower encoded scans.
- Describe CLP's applicability and limits without repeating the final Variant-versus-CLP decision. Stable schemas, nearly unique shapes, or nearly unique text leave less repetition for CLP to exploit.

### 3. From Predicate to Physical Scan: Parquet Variant and CLP

- Link the August 2025 walkthrough instead of repeating UDF rewriting or optimizer mechanics. Begin after pushdown and ask which physical data the reader touches.
- Use one four-row table: exact named path, one path with multiple types, cross-field typed value search, and wildcard key/path matching.
- Mark the capability boundary in the table. Cross-field typed value search is current connector behavior. General wildcard key/path matching is future connector work, although the MST can identify candidate paths before event scans.
- Scope Variant cost precisely: unshredded operations inspect relevant `value` pages and row-local binary structure, not textual JSON or unrelated columns.
- Do not repeat the table in prose. Transition directly to cross-file metastore planning and metadata inside each selected CLP archive.

### 4. Beyond File Pruning: Leveraging Metastore and CLP Metadata

- Follow one sequence: general metastore planning → workload-driven early termination → CLP archive metadata.
- Begin with general file-level planning. A metastore identifies candidate objects across CLP's two storage forms: compressed KV-IR files that are appendable, streamable, and searchable, and compact immutable archives. State that both forms are designed for streaming search. The metastore uses time bounds, dimensions, statistics, and related metadata before worker splits are created. This capability is complementary to CLP and common across analytical metastores.
- State that Uber applies the metastore planning across billions of file records. Keep the scale rounded, omit exact infrastructure sizing, and do not present Uber's metastore as the OSS reference provider.
- Explain the conventional boundary before presenting Uber's specialization. Presto's distributed Top-N normally limits output, not scan scope. In the conventional Hive-style path, it sits above split discovery and cannot tell the split source when enough data has been found. An unseen split may contain a later qualifying row, so every file surviving ordinary pruning remains eligible. Scope this claim to conventional Hive-style split discovery rather than every possible Presto connector.
- Explain Uber's Top-N file skipping in plain language and keep it distinct from pruning. Pruning asks whether one file can match. The Top-N optimization asks whether any remaining older file can change the latest `N` results, even though those files may contain matches.
- Give the cutoff mechanics: descending maximum-timestamp order, exact matching-event counts for metadata-resolvable filters, the earliest covered event timestamp as a watermark, retention of every overlapping file, and safe fallback when row-level predicates prevent an exact count. The result is that excluded older files never become splits or open object-storage streams.
- State that accurate time bounds and counts are required. Append-only, time-ordered files keep bounds stable and often reduce overlap, making the optimization more effective, but do not make the cutoff correct by themselves.
- Label Uber's internal metastore and eligible Top-N implementation as production extensions, not current OSS.
- End with metadata inside the selected CLP archive. Treat ordinary column statistics, value dictionaries, and indexes as a shared baseline rather than an either/or contrast. What distinguishes CLP is the archive-local MST, which provides the complete path/type-to-column map, plus log-template metadata that captures structure inside text values. These are not part of the standard Parquet Variant representation.
- State the ownership boundary directly. Metastore metadata determines which files become splits. CLP archive metadata narrows the work inside a selected split.
- After the two metadata layers, allow only rounded production observations: a roughly 30× raw-to-compressed ratio across Uber's enriched workload mix, high-cardinality identifiers such as trace IDs that compress poorly, and a handful of Presto Native workers querying object storage. State explicitly that these are deployment observations, not a controlled benchmark against another format.
- Avoid repeating the predicate-binding comparison from Section 3. Focus on how a worker initializes from compact archive metadata and streams candidate columns without local staging. Omit speculative semantic-analysis directions from the reader-facing draft.

### 5. Deploying and Querying with the CLP Connector

- Lead with one compact query showing typed exact paths, typed wildcard value search, and whole-event reconstruction. Mark names as examples and refer mechanics to the earlier blog.
- Explain that typed CLP accessors bridge the current PrestoDB type system. Once Presto supports Variant operators end to end, the connector can expose the same paths through standard Variant syntax without changing CLP storage or physical execution.
- Define the optional convenience schema here: selected CLP paths can appear as ordinary typed Presto columns without rewriting archives because the ERT columns already exist.
- Then give the operator path only: ingest through the YScope CLP OSS package, install matched coordinator/worker artifacts, enable the worker loader, and configure `etc/catalog/clp.properties`.
- Explain that the [documented production path](https://docs.yscope.com/presto/connector/clp.html) reads the MySQL metadata populated by CLP ingestion. Custom metadata providers may replace the default MySQL-backed provider.
- State the current boundary: read-only access through the `default` schema.
- Link the [packaging flow](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/tools/build-packages/README.md). Do not turn the section into an installation guide.
- Link the connector's [Compose stack](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/integration-tests/docker-compose.yaml) as the concrete matched-artifact and init-container example. Link the CLP [deployment Compose stack](https://github.com/jackluo923/clp/blob/cf9e2fab8e307a90a1009364c7a21d7a7b0a1727/tools/deployment/presto-clp/docker-compose.yaml) as the complete coordinator/worker deployment example.
- End by revealing that the short operator path depends on a two-part coordinator–worker contract.

### 6. Building Pluggable Presto Native Connectors: Lessons from CLP

- State that YScope engineers collaborated with the Presto community to build the CLP connector as one of the first end-to-end, out-of-band connector plugins for Presto Native, subject to the final chronology check.
- Explicitly contrast the earlier in-tree integration at the Velox execution-framework layer, with corresponding Presto/Prestissimo extensions, against the current out-of-band library at the Presto Native worker-plugin boundary. Never call the earlier implementation a plugin or non-native.
- Show Java coordinator responsibilities—metadata, tables, splits, rewriting, and handle serialization—versus native worker responsibilities—registration, deserialization, CLP scanning, and columnar batches.
- Use one diagram: SQL client → Java coordinator plugin → native worker loader/CLP `.so` → CLP storage.
- Explain the two upstream hooks. [PR #27653](https://github.com/prestodb/presto/pull/27653) registers factories from dynamically loaded code. [PR #27652](https://github.com/prestodb/presto/pull/27652) routes connector-specific serialized values to native deserialization.
- Frame the implementation guidance as three lessons learned from building CLP, then move from architecture to three reusable stages without restarting the story:
  1. **Define and implement the contract:** tables, types, splits, pushdowns, residuals, and cross-boundary handles. Cover the Java plugin/factory/codecs plus native `registerExtensions`, adapter, version script, and `plugin.dir`. Mention `use-connector-provided-serialization-codecs`.
  2. **Package one compatible unit:** separate JAR and `.so`, adjacent dependency `lib/`, `$ORIGIN/lib`, manylinux baseline, package/tar/init-container delivery, and complete CPU/Presto/Velox compatibility. Retain the Folly `F14LinkCheck`/Velox `get_cxx_flags` lesson.
  3. **Test the deployed boundary:** query-runner tests for Java planning and serialization, standalone tests for dependency-free native helpers, and a real worker for plugin-bound behavior. Use cumulative [PR #64](https://github.com/y-scope/clp-plugin-presto-connector/pull/64) and `task integration-tests:run` as the current example.
- State the historical takeaway once near the beginning: native execution moved out of the fork, not out of the worker. End on one release unit: coordinator artifact, native bundle, and serialized protocol.

Keep the publication check in this outline rather than the reader-facing draft. Replace PR #64 with stable merged documentation and confirm the release matrix.

### 7. Conclusion: Choosing Semi-Structured Storage for Presto

- Give the decision rule once. Variant fits cross-engine portability with predictable paths for shredding. CLP fits when the analytical platform cannot control upstream schemas or content or preselect useful query paths.
- Retain the stable-schema and near-unique-shape limits without repeating the storage-model inventory.
- State the future logical/physical distinction concretely. CLP accessors can switch to standard Variant operators once PrestoDB supports them end to end, but scans remain different: Parquet reads `value` or shredded columns, while CLP binds paths to ERT columns and encoded text streams.
- End on complementarity and the Presto contribution: common SQL and planning can coexist with storage-specific native execution behind a pluggable connector boundary.

### Acknowledgments

Credit Jack Luo, Kirk Rodrigues, Rui Wang, Xiaochong Wei, and Chenxing Yang for leading the connector implementation at YScope. Credit Pratik Joseph Dabre, Matt Graham, Deepak Majeti, and Tim Meehan for IBM/Presto infrastructure and review. Credit Karthik Bhat, Qingqin Hou, Curt Hu, Maksym Ivanchenko, Jianjian Xie, and Yun Zhang at Uber, together with YScope engineers, for extending and applying the architecture in production.

---

## Information Design

Use only three aids: the predicate table in Section 3, the compact query in Section 5, and the coordinator/worker diagram in Section 6. Do not add a benchmark dashboard, second layout diagram, full configuration block, optimizer walkthrough, or exhaustive API inventory.

---

## Evidence Labels

| Label | Meaning | Examples |
|---|---|---|
| **Prior foundation** | Established by the August 2025 blog or earlier CLP research | ERTs, log templates (CLP logtypes), typed accessors, rewriting, pushdown, conventional-Parquet evaluation |
| **Current OSS** | Verifiable in current public source | CLP KV-IR/archive and mixed scans, typed exact paths, wildcard value search, whole-event reconstruction, pluggable metadata |
| **Uber production** | Deployed behavior not necessarily public | Tens-of-petabytes monthly scale with headroom, billion-file metastore, roughly 30× raw-to-compressed ratio, handful of workers, early termination |
| **Future direction** | Architecturally compatible but not promised | `event VARIANT`, structural/semantic summaries |
| **Comparative claim** | Requires a controlled evaluation | Compression or query-performance ordering versus Parquet Variant |

---

## Non-Negotiable Safeguards

- Keep the CLP connector primary and Variant supportive. The article is neither a Variant rebuttal nor a connector advertisement.
- Avoid universal or sales language such as “superior” or “best.” Every advantage must name the workload condition that creates it and the condition under which it fades.
- State the availability asymmetry. CLP works in PrestoDB through the external connector. PrestoDB does not yet support Variant end to end.
- Distinguish logical complementarity from physical equivalence. Future Variant SQL could sit over CLP without changing CLP storage.
- Describe the earlier implementation as native execution in Velox workers of a Presto fork requiring fork/Velox changes. Never call it an external plugin or non-native.
- Describe the current worker component as an out-of-band shared library loaded by Presto Native.
- Call unshredded Variant a native binary row-local value, not textual JSON, an opaque blob, a row store, or literal “row compression.”
- Do not claim Variant lacks pushdown, cannot express recursive predicates, or cannot preserve type-polymorphic values. Scope exhaustive scans to relevant `value` pages and per-row binary structure.
- CLP is intrinsically columnized. “All fields shredded” is only an analogy. For changing shapes, comprehensive shredding still creates a file-wide sparse union. For stable schemas, do not claim a field-columnization advantage over typed/comprehensively shredded Parquet.
- Do not say that CLP has no null values. Distinguish explicit source nulls from structurally absent fields, and note that Parquet encodes missing entries through definition levels rather than literal null payloads.
- Tie CLP advantages to recurring structures, static text interleaved with variable values, unpredictable fields/types, or cross-field search. Coalescing near-singleton ERTs controls overhead but cannot create structural repetition.
- Never apply the August 2025 conventional-Parquet benchmark results to Variant or shredding. Those results predate the later Variant stack. A direct ordering requires the same corpus, codec, shredding policy, query set, and reader versions.
- Describe current typed value search across fields without naming an exact field. Do not call this wildcard-key matching or imply a broader wildcard-projection API.
- Separate current OSS, Uber production, and future metadata behavior. Keep semantic summaries future. Distinguish pruning from early termination.
- Keep compressed KV-IR files (appendable, streamable, searchable), immutable archives, and mixed scans distinct. ERT and archive-local MST mechanics describe the immutable archive path.
- Qualify “build once, run everywhere” with libc, architecture, dependency, Presto Native/Velox, and target-CPU compatibility. Folly's `F14LinkCheck` makes CPU flags part of the loading contract.
- Divide testing by runtime dependency. Pure native helpers may be unit-tested. Plugin-bound code whose symbols come from the worker needs real-worker integration.
- Use only rounded production observations as evidence of operation at scale: tens of petabytes per month with qualitative headroom, billions of file records, a roughly 30× raw-to-compressed ratio on enriched logs, and a handful of workers. Do not include exact ingest, compressed volume, core count, latency, or throughput. Never present these observations as a controlled comparison with another format. Preserve YScope, IBM/Presto-community, and Uber attribution.
- Do not add the Pinot analogy or Uber's broader hot-tier architecture. Neither advances the Presto connector thesis.

---

## Verify Before Publication

- Variant chronology: Iceberg v3 adoption, Parquet Variant/shredding approval dates, and current Trino support status.
- Current PrestoDB Variant status across Java, serialization, Presto Native/Velox, Iceberg, and Parquet reading.
- Exact historical/current connector boundary and the merge state/capabilities of Presto PRs #27653 and #27652.
- Confirm current public CLP functions, cross-field typed value-search semantics, and the future boundary for wildcard key/path matching.
- “One of the first” versus “the first” out-of-tree, dynamically loaded Presto Native connector.
- Final release matrix: Presto/Velox pins, images, libc, architectures, CPU targets, and `F14LinkCheck` mode.
- Confirm the merge and stable documentation for cumulative connector PR #64. Replace the draft's PR link.
- After the deployment PRs land, verify both Compose links and replace the pinned `jackluo923/clp` URL with the canonical `y-scope/clp` main-branch path.
- Fact-check Uber's rounded scale, headroom, billion-file, compression-ratio, and handful-of-workers statements, together with metadata claims, acknowledgments, and future-direction wording. Detailed capacity and infrastructure figures remain out of scope.

---

## Out of Scope

- A full Variant, Iceberg v3, Trino, CLP-format, or μSlope tutorial.
- Reproduction of the earlier optimizer, split-discovery, pushdown, or benchmark walkthrough.
- A conventional-Parquet benchmark presented as evidence about Variant.
- Uber's complete ingestion or hot-tier architecture.
- A full SQL API, installation, build-troubleshooting, or configuration guide.
- Unverified performance multipliers, semantic-search costs, or product commitments.
