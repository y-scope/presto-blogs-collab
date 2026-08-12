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

1. **Storage decision:** Explain the physical difference between Variant with optional shredding and CLP's native columnar model.
2. **Connector evolution:** Show how native execution moved from modifications in a Presto fork and Velox to an out-of-band shared library.
3. **Community value:** Give operators a short usage path and connector authors a compact, reusable bootstrap pattern.

Readers should leave able to decide when portability and predictable hot paths favor Variant, when uncontrolled recurring structures and cross-field or encoded-text search favor CLP, and which capabilities are current OSS, Uber production extensions, or future directions.

---

## Narrative Map and Budget

| Section | Editorial job | Target |
|---|---|---:|
| Opening | Lead with the unresolved physical layer, production stakes, prior post, and current connector | 220–250 |
| 1. Variant landscape | Explain the SQL/table/file stack and unshredded-versus-shredded tradeoff | 210–235 |
| 2. CLP model | Explain what specialized storage contributes and when it matters | 320–350 |
| 3. Predicate binding | Compare the physical work behind exact, polymorphic, and cross-field predicates | 200–225 |
| 4. Metadata beyond pruning | Use metastore and archive metadata as production evidence | 230–250 |
| 5. Deployment to first query | Show the SQL outcome first, then the short operator path | 200–225 |
| 6. Pluggable connector reference | Explain the Java/native contract and compact reusable author guidance | 400–440 + diagram |
| 7. Conclusion | Make the workload decision and complementary-model thesis once | 130–160 |
| Acknowledgments | Credit implementation, infrastructure, review, and production application | 35–50 |

Each section must create the question answered by the next: landscape → storage model → query work → metadata → operator experience → reusable connector architecture → decision.

---

## Prior Foundation

YScope's August 2025 post, [“Querying CLP-compressed logs with SQL via Presto”](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82), owns the original motivation and the introductions to Encoded Record Tables (ERTs), Merged Schema Trees (MSTs), and logtypes. It also owns the typed-accessor design, UDF rewriting, split discovery, pushdown mechanics, and evaluation against conventional Parquet.

This article may define ERTs, the MST, and logtypes briefly because the Variant comparison requires them, and may include one compact current-connector query. It must not reproduce the earlier optimizer walkthrough, metadata-query generation, plan diagrams, or benchmarks. Cite the earlier post at the first CLP–Presto history discussion.

---

## Section Contracts

### Opening: SQL Abstraction Does Not Erase Storage Design

- Lead with the tension: Variant standardizes interchange, but a value type and table contract do not determine physical organization, compression, or search.
- Frame schema variability as an ownership problem: analytical platforms do not control evolving producers or future query paths.
- Define the open-source [CLP project](https://github.com/y-scope/clp) from [YScope](https://yscope.com) as a native columnar format and search engine. Use Uber's roughly 25 PB/month raw volume to establish stakes, not as comparative benchmark evidence. Cite the [OSDI '24 μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui) for core storage ideas, then the August 2025 post for the original Presto integration.
- State the history precisely: the earlier implementation executed CLP natively in Velox workers of a Presto fork and required changes in that fork and Velox.
- Introduce the present contrast: PrestoDB lacks end-to-end Variant, while CLP is available through a pluggable Presto Native connector.
- End with the article's two questions: where the physical work differs and how the connector carries CLP from SQL to the coordinator–worker boundary.

### 1. Variant in the Lakehouse Landscape

- Separate the layers. The engine defines the dynamic SQL type. [Iceberg v3](https://iceberg.apache.org/spec/#variant-type) defines the table type. Parquet defines the binary [`metadata`/`value` encoding](https://parquet.apache.org/docs/file-format/types/variantencoding/) and optional [shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/).
- Use [Trino Variant](https://trino.io/docs/current/functions/variant.html) only as implementation precedent in the Presto lineage, not as a deployable PrestoDB feature or the comparison target.
- Describe unshredded Variant as a row-local native binary value: known paths are navigable without parsing text, but inner fields lack independent Parquet columns and statistics.
- Explain that shredding gives writer-selected paths typed columns, compression, projection, and statistics. The choice must be set before the writer opens each file. Adding paths requires rewriting existing files.
- Preserve type-polymorphism accuracy: one `typed_value` has one declared type, while other types remain in `value`. Promoting every path/type creates a wide, sparse physical union.
- Clarify that even fully shredding all observed paths retains one file-wide schema. Fields absent from a row become missing optional-column entries. Parquet encodes this sparsity compactly through definition levels rather than literal null payloads.
- Use the Parquet [overview](https://parquet.apache.org/blog/2026/02/27/variant-type-in-apache-parquet-for-semi-structured-data/) and Databricks [introduction](https://www.databricks.com/blog/introducing-variant-new-open-standard-semi-structured-data-apache-parquettm-delta-lake) as background. Specifications support normative claims.

### 2. CLP: Native Columnar Semi-Structured Storage

- Open with a direct reference to YScope's [August 2025 post](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) for the full explanation. State that only two ideas repeat here: document structure through ERTs/MST and text decomposition through logtypes/typed value columns.
- Establish the physical contrast with a table-within-a-file analogy. A Parquet file remains one table whose rows contain Variant values. Optional shredding adds writer-selected typed columns to that table. A CLP archive is closer to a container of many shape-specific columnar subtables behind one logical event stream. Avoid “nested tables,” which could imply a SQL-visible hierarchy.
- Define an encoded record table (ERT): each ERT groups one recurring key-and-type shape into type-homogeneous encoded columns. Each immutable CLP archive discovers and defines its own ERTs independently. Its archive-local **Merged Schema Tree (MST)** merges those schemas into one inventory and maps every observed path/type to their columns, including separate columns for polymorphic types. ERTs are internal storage structures, not separately addressable SQL tables.
- Contrast structural absence with explicit nulls. Each ERT contains only the fields in its shape, avoiding missing placeholders across unrelated shapes. Explicit source null values are still preserved.
- Treat the MST as archive-local execution metadata rather than a global table schema. Move the convenience schema to Section 5 so it does not interrupt the ERT → MST → logtype reveal.
- Explain logtypes once as inferred, format-string-like templates that encode recurring static text once while CLP stores the extracted variable values in separate typed ERT columns. This enables encoded free-text search and log-specific compression below the document-field boundary. Connect shape and logtype locality to query work: structural predicates narrow scans to matching ERTs/columns, while text predicates search encoded logtypes/variables.
- Give the decision rule explicitly. CLP fits unpredictable paths/types, recurring uncontrolled schemas, cross-field/free-text search, and compact searchable archives. Variant with selective shredding fits interoperable tables and predictable hot paths.
- State limitations. With a stable schema, CLP has no field-columnization advantage over typed or comprehensively shredded Parquet. With near-singleton shapes, recurring-shape locality fades.

### 3. From Predicate to Physical Scan: Parquet Variant and CLP

- Link the August 2025 walkthrough instead of repeating UDF rewriting or optimizer mechanics. Begin after pushdown and ask which physical data the reader touches.
- Keep one three-row table comparing exact named paths, polymorphic paths, and typed cross-field value search.
- Scope Variant cost precisely: an unshredded recursive predicate reads relevant `value` pages and inspects per-row binary structure, not unrelated columns or textual JSON.
- State current CLP behavior plainly: when a query uses a wildcard rather than an exact key, the connector infers the value type and resolves compatible encoded columns from split metadata.
- Close by introducing the two metadata layers behind those scans: cross-file metastore planning and metadata inside each selected CLP archive. Leave future Variant complementarity to the conclusion.

### 4. Beyond File Pruning: Leveraging Metastore and CLP Metadata

- Follow one sequence: general metastore planning → workload-driven early termination → CLP archive metadata.
- Begin with general file-level planning. A provider identifies candidate objects across CLP's two storage forms: compressed KV-IR files that are appendable, streamable, and searchable, and compact immutable archives. State that both forms are designed for streaming search. The provider uses time bounds, dimensions, statistics, and related metadata before worker splits are created. This capability is complementary to CLP and common across analytical metastores.
- Give Uber's billion-file internal-metastore scale and resource efficiency qualitatively. Do not publish its infrastructure footprint or present it as the OSS reference provider.
- Then explain the stock boundary before presenting the specialization. Presto's distributed Top-N normally limits output, not scan scope. In the conventional Hive-style path, it sits above split discovery and cannot tell the split source when enough data has been found. An unseen split may contain a newer qualifying row, so every file surviving ordinary pruning remains eligible. Scope this claim to conventional Hive-style split discovery rather than every possible Presto connector. Uber's file-enumeration termination needs trustworthy beginning and ending timestamps for each candidate data group, newest-first enumeration, exact matching counts for supported file-level predicates, an overlap-aware cutoff, and safe fallback. Emphasize the result: once the proof succeeds, older files never become splits, never reach workers, and never open object-storage streams. Time-sorted files and append-only publication are helpful, not defining requirements. These proof inputs are not normally part of a general metastore interface.
- Label Uber's billion-file metastore and eligible Top-N implementation as production extensions, not current OSS. State that early termination is not a second form of pruning. Pruning independently removes a file only when it cannot match and does not depend on ordering or a limit. Top-N termination stops enumerating remaining older files once they cannot alter the newest `N`, even though those files may match. Paraphrase the proof requirements: exact counts for metadata-resolvable predicates, descending maximum-timestamp order, an overlap-aware cutoff, and safe fallback for unsupported predicates. Then refer to Uber's CLP Part III article for the complete correctness explanation.
- End with metadata inside the selected CLP archive. Treat ordinary column statistics, value dictionaries, and indexes as a shared baseline rather than an either/or contrast. What distinguishes CLP is the archive-local MST, which provides the complete path/type-to-column map, plus log-template metadata that captures structure inside text values. These are not part of the standard Parquet Variant representation.
- State the ownership boundary directly. Metastore metadata determines which files become splits. CLP archive metadata narrows the work inside a selected split.
- Add the approved production proof after the two metadata layers: about 25 PB/month raw to 750–850 TB, with five 48-core workers serving CLP data from object storage.
- Avoid repeating the predicate-binding comparison from Section 3. Focus on how a worker initializes from compact archive metadata and streams candidate columns without local staging. Omit speculative semantic-analysis directions from the reader-facing draft.

### 5. Deploying and Querying with the CLP Connector

- Lead with one compact query showing typed exact paths, typed wildcard value search, and whole-event reconstruction. Mark names as examples and refer mechanics to the earlier post.
- Explain that typed CLP accessors bridge the current PrestoDB type system. Once Presto supports Variant operators end to end, the connector can expose the same paths through standard Variant syntax without changing CLP storage or physical execution.
- Define the optional convenience schema here: selected CLP paths can appear as ordinary typed Presto columns without rewriting archives because the ERT columns already exist.
- Then give the operator path only: ingest through the YScope CLP OSS package, install matched coordinator/worker artifacts, enable the worker loader, and configure `etc/catalog/clp.properties`.
- Explain that the [documented production path](https://docs.yscope.com/presto/connector/clp.html) reads the MySQL metadata populated by CLP ingestion. Custom providers may replace it.
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
- Move from architecture to three reusable stages without restarting the story:
  1. **Define and implement the contract:** tables, types, splits, pushdowns, residuals, and cross-boundary handles. Cover the Java plugin/factory/codecs plus native `registerExtensions`, adapter, version script, and `plugin.dir`. Mention `use-connector-provided-serialization-codecs`.
  2. **Package one compatible unit:** separate JAR and `.so`, adjacent dependency `lib/`, `$ORIGIN/lib`, manylinux baseline, package/tar/init-container delivery, and complete CPU/Presto/Velox compatibility. Retain the Folly `F14LinkCheck`/Velox `get_cxx_flags` lesson.
  3. **Test the deployed boundary:** query-runner tests for Java planning and serialization, standalone tests for dependency-free native helpers, and a real worker for plugin-bound behavior. Use cumulative [PR #64](https://github.com/y-scope/clp-plugin-presto-connector/pull/64) and `task integration-tests:run` as the current example.
- State the historical takeaway once near the beginning: native execution moved out of the fork, not out of the worker. End on the deployable unit: coordinator artifact, native bundle, and serialized protocol together.

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
| **Prior foundation** | Established by the August 2025 post or earlier CLP research | ERTs, log templates (CLP logtypes), typed accessors, rewriting, pushdown, conventional-Parquet evaluation |
| **Current OSS** | Verifiable in current public source | CLP KV-IR/archive and mixed scans, typed exact paths, wildcard value search, whole-event reconstruction, pluggable metadata |
| **Uber production** | Deployed behavior not necessarily public | Scale figures, advanced metastore, early termination |
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
- Describe current typed value search over wildcard keys without expanding into a broader wildcard-projection API discussion.
- Separate current OSS, Uber production, and future metadata behavior. Keep semantic summaries future. Distinguish pruning from early termination.
- Keep compressed KV-IR files (appendable, streamable, searchable), immutable archives, and mixed scans distinct. ERT and archive-local MST mechanics describe the immutable archive path.
- Qualify “build once, run everywhere” with libc, architecture, dependency, Presto Native/Velox, and target-CPU compatibility. Folly's `F14LinkCheck` makes CPU flags part of the loading contract.
- Divide testing by runtime dependency. Pure native helpers may be unit-tested. Plugin-bound code whose symbols come from the worker needs real-worker integration.
- Use production figures only as evidence of operation at scale. Preserve YScope, IBM/Presto-community, and Uber attribution.
- Do not add the Pinot analogy or Uber's broader hot-tier architecture. Neither advances the Presto connector thesis.

---

## Verify Before Publication

- Variant chronology: Iceberg v3 adoption, Parquet Variant/shredding approval dates, and current Trino support status.
- Current PrestoDB Variant status across Java, serialization, Presto Native/Velox, Iceberg, and Parquet reading.
- Exact historical/current connector boundary and the merge state/capabilities of Presto PRs #27653 and #27652.
- Confirm current public CLP functions and typed wildcard-key search semantics.
- “One of the first” versus “the first” out-of-tree, dynamically loaded Presto Native connector.
- Final release matrix: Presto/Velox pins, images, libc, architectures, CPU targets, and `F14LinkCheck` mode.
- Confirm the merge and stable documentation for cumulative connector PR #64. Replace the draft's PR link.
- After the deployment PRs land, verify both Compose links and replace the pinned `jackluo923/clp` URL with the canonical `y-scope/clp` main-branch path.
- Resolve `UBER_BLOG_URL` against publication order. If Uber's Part III article is not live when the Presto article publishes, label it forthcoming and add the link when available.
- Approval for Uber figures, worker count, metadata claims, acknowledgments, and future-direction wording.

---

## Out of Scope

- A full Variant, Iceberg v3, Trino, CLP-format, or μSlope tutorial.
- Reproduction of the earlier optimizer, split-discovery, pushdown, or benchmark walkthrough.
- A conventional-Parquet benchmark presented as evidence about Variant.
- Uber's complete ingestion or hot-tier architecture.
- A full SQL API, installation, build-troubleshooting, or configuration guide.
- Unverified performance multipliers, semantic-search costs, or product commitments.
