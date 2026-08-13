# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

- **Status:** Editorial outline synchronized with the current draft
- **Audience:** Presto users, operators, maintainers, and external connector authors
- **Target:** 2,500–2,700 words
- **Primary subject:** The current pluggable CLP connector for Presto Native
- **Supporting background:** Variant as the standardized lakehouse reference for semi-structured data
- **Point of view:** Presto users and maintainers deciding when specialized storage belongs behind a common SQL interface

---

## Editorial Contract

The article explains what specialized storage can contribute beneath a standard semi-structured SQL interface, then shows how Presto Native exposes CLP through a pluggable connector. Variant supplies the broader lakehouse context. It is not a currently available PrestoDB alternative or a co-equal subject.

> CLP's distinctive storage model combines intrinsic columnization, recurring-shape locality, log-specific encoding, and storage-native search. The connector exposes those capabilities behind a reusable Presto Native extension boundary. A future Variant SQL value could complement CLP logically without making Parquet Variant and CLP physically equivalent.

Keep this hierarchy:

1. **CLP storage model:** Use Variant briefly as a familiar lakehouse baseline, then explain CLP's intrinsic columnization, recurring-text encoding, and storage-native search.
2. **Connector evolution:** Show how native execution moved from an in-tree Presto Native/Velox integration that required a fork to an out-of-band shared library.
3. **Community value:** Give operators a short usage path and connector authors a compact, reusable bootstrap pattern.

Readers should leave able to decide when portability and predictable hot paths favor Variant, when uncontrolled recurring structures and cross-field or encoded-text search favor CLP, and which capabilities are current OSS, Uber production extensions, or future directions.

---

## Narrative Map and Budget

| Section | Editorial job | Target |
|---|---|---:|
| Opening | Lead with Presto's storage boundary, production stakes, prior blog, and current connector | 285–305 |
| 1. Variant landscape | Establish only the SQL/table/file layers and the shredding decision | 115–130 |
| 2. CLP model | Explain CLP's physical model, benefits, and workload-dependent limits | 285–310 |
| 3. Predicate binding | Use one table to distinguish exact paths, polymorphism, cross-field value search, and broader wildcard matching | 185–210 |
| 4. Metadata beyond pruning | Use metastore and archive metadata as production evidence | 550–590 |
| 5. Deployment to first query | Show the SQL outcome first, then the short operator path | 310–335 |
| 6. Pluggable connector reference | Explain the Java/native contract and compact reusable author guidance | 570–610 + diagram |
| 7. Conclusion | Give the practical storage choice and connector lesson once | 110–125 |
| Acknowledgments | Credit implementation, infrastructure, review, and production application | 70–90 |

Each section must create the question answered by the next: landscape → storage model → query work → metadata → operator experience → reusable connector architecture → decision.

---

## Prior Foundation

YScope's August 2025 blog, [“Querying CLP-compressed logs with SQL via Presto”](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82), owns the original motivation, storage-model overview, typed-accessor design, UDF rewriting, split discovery, pushdown mechanics, and evaluation against conventional Parquet. The [μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui) provides the formal background for Encoded Record Tables (ERTs), Merged Schema Trees (MSTs), and logtypes.

This article may define ERTs, the MST, and logtypes briefly because readers need them to understand CLP's physical model, and may include one compact current-connector query. It must not reproduce the earlier optimizer walkthrough, metadata-query generation, plan diagrams, or benchmarks. Cite the earlier blog at the first CLP–Presto history discussion.

---

## Section Contracts

### Opening: SQL Abstraction Does Not Erase Storage Design

- Lead with Presto's abstraction boundary: common SQL does not make physical storage layouts interchangeable.
- Frame schema variability as an ownership problem: analytical platforms do not control evolving applications or future query paths.
- Define the open-source [CLP project](https://github.com/y-scope/clp) from [YScope](https://yscope.com) as a native columnar format and search engine. Establish operating scale with Uber's tens of petabytes of raw logs per month and substantial deployment headroom, not detailed infrastructure figures or comparative benchmark evidence. Cite the [OSDI '24 μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui) for the storage model.
- Cite YScope's August 2025 blog for the original Presto integration. State the history precisely: it executed CLP natively through Velox in a Presto fork and was not an external plugin.
- Present the current pluggable connector as the article's subject. Introduce Variant only as a brief lakehouse reference that, as of Q3 2026, PrestoDB does not yet support end to end.
- End by promising a short Variant baseline followed by CLP's storage model and coordinator–worker integration.

### 1. Variant in the Lakehouse Landscape

- Define only the baseline needed later: engines provide the dynamic SQL type and operators, [Iceberg v3](https://iceberg.apache.org/spec/#variant-type) defines the table type, and Parquet defines the binary [`metadata`/`value` encoding](https://parquet.apache.org/docs/file-format/types/variantencoding/) and optional [shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/).
- Describe unshredded Variant as a row-local native binary value.
- Explain that shredding gives writer-selected paths independent typed columns for compression, projection, statistics, and skipping. Writers choose those paths before opening the file; adding them later requires rewriting it.
- End by handing the question to CLP: what can specialized storage do when useful paths and types cannot be predicted?
- Keep implementation history and extended background reading out of the narrative. Use specifications as links on the concepts they support.

### 2. CLP: Native Columnar Semi-Structured Storage

- Open with a direct reference to YScope's [August 2025 blog](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) for the original storage overview and Presto walkthrough, and cite the μSlope paper for the formal ERT, MST, and logtype model. Repeat only the two physical ideas needed here: document structure through ERTs and the MST, and recurring-text decomposition through logtypes and typed value columns.
- Use one brief physical contrast: Parquet Variant begins with row-local values and selected shredded columns, while a CLP archive contains many shape-specific columnar subtables behind one logical event stream. Avoid “nested tables,” which could imply a SQL-visible hierarchy.
- Define an encoded record table (ERT) and the archive-local Merged Schema Tree (MST). Make clear that they are archive-local storage structures, not SQL tables or a global schema.
- Explain that ERTs omit structurally absent fields while preserving explicit source nulls.
- Explain logtypes once and connect recurring structure, text, and values to compression and narrower encoded scans.
- Describe CLP's applicability and limits without repeating the final Variant-versus-CLP decision. Stable schemas, nearly unique shapes, or nearly unique text leave less repetition for CLP to exploit.

### 3. From Predicate to Physical Scan: Parquet Variant and CLP

- Link the August 2025 walkthrough instead of repeating UDF rewriting or optimizer mechanics. Begin after pushdown and ask which physical data the reader touches.
- Use one five-row table: exact named path, one path with multiple types, cross-field typed value search, wildcard key/path matching, and recurring-text or pattern search.
- Mark the capability boundary in the table. Cross-field typed value search is current connector behavior. Broader arbitrary wildcard key/path support exists in deployment and is being prepared for open-source release; the MST identifies candidate paths before event scans.
- Scope Variant cost precisely: unshredded operations inspect relevant `value` pages and row-local binary structure, not textual JSON or unrelated columns.
- Do not repeat the table in prose. Transition directly to cross-file metastore planning and metadata inside each selected CLP archive.

### 4. Beyond File Pruning: Leveraging Metastore and CLP Metadata

- Open with three stages: ordinary metastore pruning → Uber's latest-`N` planning → CLP archive metadata inside each selected split.
- Begin with general file-level planning. The native connector can scan two CLP storage forms when its split provider supplies them: compressed KV-IR files that are appendable, streamable, and searchable, and compact immutable archives. State that both support streaming reads while KV-IR can continue growing. The shipped MySQL provider currently emits archive splits; Uber's internal metastore plans across both forms. A metastore uses time bounds, dimensions, statistics, and related metadata before worker splits are created.
- State that Uber applies the metastore planning across billions of file records. Keep the scale rounded, omit exact infrastructure sizing, and do not present Uber's metastore as the OSS reference provider.
- Explain the conventional boundary before presenting Uber's specialization. Presto's distributed Top-N normally limits output, not scan scope. In the conventional Hive-style path, it sits above split discovery and cannot tell the split source when enough data has been found. An unseen split may contain a later qualifying row, so every file surviving ordinary pruning remains eligible. Scope this claim to conventional Hive-style split discovery rather than every possible Presto connector.
- Explain Uber's Top-N file skipping in plain language and keep it distinct from pruning. Pruning asks whether one file can match. The Top-N optimization asks whether any remaining older file can change the latest `N` results, even though those files may contain matches.
- Give Uber's correctness-preserving cutoff mechanics: candidate files received in descending maximum-timestamp order, exact matching-event counts for metadata-resolvable filters such as application or host, the minimum event-time bound among files considered so far as a watermark, retention of every overlapping file, and safe fallback when row-level predicates prevent an exact count. Because candidates arrive in order, the metastore can stop consuming older metadata as well as stop creating splits. Excluded older files never open object-storage streams.
- State that accurate time bounds and counts are required. Append-only, time-ordered files keep bounds stable and often reduce overlap, making the optimization more effective, but do not make the cutoff correct by themselves.
- Label Uber's internal metastore and eligible Top-N implementation as production extensions, not current OSS.
- End with metadata near the beginning of the selected CLP archive. Treat ordinary column statistics, value dictionaries, and indexes as a shared baseline rather than an either/or contrast. What distinguishes CLP is the archive-local MST, which provides the complete path/type-to-column map, plus log-template metadata that captures structure inside text values. These are not part of the standard Parquet Variant representation.
- State the ownership boundary directly. The first two stages happen during metastore planning; CLP archive metadata narrows the work inside a selected split.
- After the three stages, allow only rounded production observations: a roughly 30× raw-to-compressed ratio across Uber's enriched workload mix, high-cardinality identifiers such as trace IDs that compress poorly, and a handful of Presto Native workers querying object storage. State explicitly that these are deployment observations, not a controlled benchmark against another format.
- Avoid repeating the predicate-binding comparison from Section 3. Focus on how a worker initializes from compact archive metadata and streams candidate columns without local staging. Omit speculative semantic-analysis directions from the reader-facing draft.

### 5. Deploying and Querying with the CLP Connector

- Lead with one compact query showing typed exact paths, typed wildcard value search, and whole-event reconstruction. Mark names as examples and refer mechanics to the earlier blog.
- State explicitly that `CLP_WILDCARD_*_COLUMN()` performs cross-field typed value search. It is distinct from broader arbitrary wildcard key/path matching and projection.
- Explain that typed CLP accessors bridge the current PrestoDB type system. Once Presto supports Variant operators end to end, the connector can expose the same paths through standard Variant syntax without changing CLP storage or physical execution.
- Define the optional convenience schema here: selected CLP paths can appear as ordinary typed Presto columns without rewriting archives because the ERT columns already exist.
- Then give the operator path only: ingest through YScope's open-source CLP package, install version-matched coordinator/worker artifacts, enable `use-connector-provided-serialization-codecs` on the coordinator, point each worker's `plugin.dir` to its native bundle, and install matching `etc/catalog/clp.properties` files.
- Explain that the [documented production path](https://docs.yscope.com/presto/connector/clp.html) uses the MySQL-backed provider populated by CLP ingestion. Metadata and split discovery sit behind provider interfaces. Another deployment can implement and wire those interfaces for its own metastore without changing the native scan. Do not imply runtime-discoverable or drop-in provider packages.
- State the current boundary: read-only access through the `default` schema.
- Link the [packaging flow](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/tools/build-packages/README.md). Do not turn the section into an installation guide.
- Link the connector's [Compose stack](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/integration-tests/docker-compose.yaml) as the concrete matched-artifact and init-container example. Link the canonical CLP [deployment Compose stack](https://github.com/y-scope/clp/blob/main/tools/deployment/presto-clp/docker-compose.yaml) as the complete coordinator/worker deployment example.
- End by revealing that the short operator path depends on a two-part coordinator–worker contract.

### 6. Building Pluggable Presto Native Connectors: Lessons from CLP

- State that YScope engineers collaborated with the Presto community to build the CLP connector as one of the first end-to-end, out-of-band connector plugins for Presto Native, subject to the final chronology check.
- Explicitly contrast the earlier in-tree integration at the Velox execution-framework layer, which lived directly in the Presto Native and Velox source trees and required a fork while those changes remained outside upstream, against the current out-of-band library at the Presto Native worker-plugin boundary. Never call the earlier implementation a plugin or non-native.
- Show Java coordinator responsibilities—metadata, tables, splits, rewriting, and handle serialization—versus native worker responsibilities—registration, deserialization, CLP scanning, and columnar batches.
- Tie the boundary back to the opening schema problem: Java provides a stable Presto interface, while the native plugin interprets split-local CLP structure without forcing every observed path and type into one global table schema.
- Use the repository-owned horizontal SVG with explicit process boundaries: the CLP Java plugin inside the coordinator, the loader and CLP `.so` inside a Presto Native worker, and object storage containing KV-IR and CLP archives outside both processes. Show Presto carrying plans, serialized handles, splits, and result pages; do not imply a connector-owned Java/native RPC.
- Explain the two upstream hooks without conflating them. [PR #27653](https://github.com/prestodb/presto/pull/27653), `plugin.dir`, and `registerExtensions` load and register the native factory. On the coordinator, `use-connector-provided-serialization-codecs` selects the connector's handle codecs. [PR #27652](https://github.com/prestodb/presto/pull/27652) routes those serialized values to the native connector adapter for deserialization.
- Frame the implementation guidance as three lessons learned from building CLP, then move from architecture to three reusable stages without restarting the story:
  1. **Define and implement the contract:** walk through the coordinator-to-worker lifecycle for table, table-layout, column, split, and transaction handles, together with pushdowns and filters Presto must still evaluate. Cover the Java plugin/factory/codecs plus native `registerExtensions`, adapter, version script, and `plugin.dir`. Mention `use-connector-provided-serialization-codecs`.
  2. **Package one compatible unit:** separate JAR and `.so`, adjacent dependency `lib/`, `$ORIGIN/lib`, manylinux baseline, package/tar/init-container delivery, and complete CPU/Presto/Velox compatibility. Retain the Folly `F14LinkCheck`/Velox `get_cxx_flags` lesson.
  3. **Test the deployed boundary:** Java unit tests for predicate conversion, plan rewriting, metadata and split listing, and codec round trips; standalone tests for dependency-free native helpers; and a real worker for code whose Presto or Velox symbols are supplied at load time. Use the stable [integration-test documentation](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/integration-tests/README.md) and `task integration-tests:run` as the current example.
- State the historical takeaway once near the beginning: native execution moved out of the fork, not out of the worker. End on one release unit: coordinator artifact, native bundle, and serialized protocol.

Keep the publication check in this outline rather than the reader-facing draft. Confirm the integration-test coverage and release matrix against the final published connector version.

### 7. Conclusion: Choosing Semi-Structured Storage for Presto

- Give the decision rule once. Variant fits cross-engine portability with predictable paths for shredding. CLP fits when the analytical platform cannot control upstream schemas or content or preselect useful query paths.
- Retain the stable-schema and near-unique-shape limits without repeating the storage-model inventory.
- State the future logical/physical distinction concretely. CLP accessors can switch to standard Variant operators once PrestoDB supports them end to end, but scans remain different: Parquet reads `value` or shredded columns, while CLP binds paths to ERT columns and encoded text streams.
- End on complementarity and state the Presto contribution directly: a common SQL interface does not require a common physical layout. Presto can own planning and distributed execution while a pluggable native connector preserves storage-specific metadata, encodings, and scan paths.

### Acknowledgments

Credit Jack Luo, Kirk Rodrigues, Rui Wang, Xiaochong Wei, and Chenxing Yang for leading the connector implementation at YScope. Credit Pratik Joseph Dabre, Matt Graham, Deepak Majeti, and Tim Meehan for IBM/Presto infrastructure and review. Credit Karthik Bhat, Qingqin Hou, Curt Hu, Maksym Ivanchenko, Jianjian Xie, and Yun Zhang at Uber, together with YScope engineers, for extending and applying the architecture in production.

---

## Information Design

Use only three aids: the predicate table in Section 3, the compact query in Section 5, and the coordinator/worker diagram in Section 6. Do not add a benchmark dashboard, second layout diagram, full configuration block, optimizer walkthrough, or exhaustive API inventory.

---

## Evidence Labels

| Label | Meaning | Examples |
|---|---|---|
| **Prior foundation** | Established by the August 2025 blog or earlier CLP research | Storage-model overview and typed accessors in the blog; formal ERT/MST/logtype model in μSlope; rewriting, pushdown, and conventional-Parquet evaluation |
| **Current OSS** | Verifiable in current public source | Native KV-IR/archive scan paths when supplied by a split provider, typed exact paths, cross-field typed value search, whole-event reconstruction, and metadata-provider interfaces |
| **Uber production** | Deployed behavior not necessarily public | Tens-of-petabytes monthly scale with headroom, billion-file metastore, roughly 30× raw-to-compressed ratio, handful of workers, early termination |
| **Future direction** | Architecturally compatible but not promised | `event VARIANT`, structural/semantic summaries |
| **Comparative claim** | Requires a controlled evaluation | Compression or query-performance ordering versus Parquet Variant |

---

## Non-Negotiable Safeguards

- Keep the CLP connector primary and Variant supportive. The article is neither a Variant rebuttal nor a connector advertisement.
- Avoid universal or sales language such as “superior” or “best.” Every advantage must name the workload condition that creates it and the condition under which it fades.
- State the availability asymmetry with a time qualifier. CLP works in PrestoDB through the external connector. As of Q3 2026, PrestoDB does not yet support Variant end to end.
- Distinguish logical complementarity from physical equivalence. Future Variant SQL could sit over CLP without changing CLP storage.
- Describe the earlier implementation as native execution in Velox workers of a Presto fork requiring fork/Velox changes. Never call it an external plugin or non-native.
- Describe the current worker component as an out-of-band shared library loaded by Presto Native.
- Call unshredded Variant a native binary row-local value, not textual JSON, an opaque blob, a row store, or literal “row compression.”
- Do not claim Variant lacks pushdown, cannot express recursive predicates, or cannot preserve type-polymorphic values. Scope exhaustive scans to relevant `value` pages and per-row binary structure.
- CLP is intrinsically columnized. “All fields shredded” is only an analogy. For changing shapes, comprehensive shredding still creates a file-wide sparse union. For stable schemas, do not claim a field-columnization advantage over typed/comprehensively shredded Parquet.
- Do not say that CLP has no null values. Distinguish explicit source nulls from structurally absent fields, and note that Parquet encodes missing entries through definition levels rather than literal null payloads.
- Tie CLP advantages to recurring structures, static text interleaved with variable values, unpredictable fields/types, or cross-field search. Coalescing near-singleton ERTs controls overhead but cannot create structural repetition.
- Never apply the August 2025 conventional-Parquet benchmark results to Variant or shredding. Those results predate the later Variant stack. A direct ordering requires the same corpus, codec, shredding policy, query set, and reader versions.
- Describe current OSS typed value search across fields without naming an exact field. Distinguish it from broader arbitrary wildcard key/path support in deployment that is being prepared for open-source release; do not imply that a general wildcard-projection API is already public.
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
- Confirm current public CLP functions and cross-field typed value-search semantics, together with the release status and public interface for broader arbitrary wildcard key/path matching.
- “One of the first” versus “the first” out-of-tree, dynamically loaded Presto Native connector.
- Final release matrix: Presto/Velox pins, images, libc, architectures, CPU targets, and `F14LinkCheck` mode.
- Confirm the final integration-test coverage and stable documentation against the connector version used for publication.
- Verify that the connector documentation URL is live and describes the pluggable connector rather than the historical in-tree integration.
- Verify both canonical Compose links against the release used for publication.
- Fact-check Uber's rounded scale, headroom, billion-file, compression-ratio, and handful-of-workers statements, together with metadata claims, acknowledgments, and future-direction wording. Detailed capacity and infrastructure figures remain out of scope.

---

## Out of Scope

- A full Variant, Iceberg v3, Trino, CLP-format, or μSlope tutorial.
- Reproduction of the earlier optimizer, split-discovery, pushdown, or benchmark walkthrough.
- A conventional-Parquet benchmark presented as evidence about Variant.
- Uber's complete ingestion or hot-tier architecture.
- A full SQL API, installation, build-troubleshooting, or configuration guide.
- Unverified performance multipliers, semantic-search costs, or product commitments.
