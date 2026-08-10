# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

- **Status:** Editorial outline synchronized with the current draft
- **Audience:** Presto users, operators, maintainers, and external connector authors
- **Target:** 2,400–2,600 words
- **Primary subject:** The current pluggable CLP connector for Presto Native
- **Supporting background:** Variant as the standardized lakehouse reference for semi-structured data
- **Point of view:** Presto users and maintainers deciding what a connector should preserve

---

## Editorial Contract

The article explains how Presto can query a specialized native semi-structured format without embedding it in a fork. Variant supplies the broader lakehouse context; it is not a currently available PrestoDB alternative or a co-equal subject.

> The CLP connector preserves intrinsic columnization, recurring-shape locality, log-specific encoding, and storage-native search behind a reusable Presto Native extension boundary. A future Variant SQL value could complement CLP logically without making Iceberg/Parquet Variant and CLP physically equivalent.

Keep this hierarchy:

1. **Storage decision:** Explain the physical difference between Variant with optional shredding and CLP's native columnar model.
2. **Connector evolution:** Show how native execution moved from modifications in a Presto fork and Velox to an out-of-band shared library.
3. **Community value:** Give operators a short usage path and connector authors a compact, reusable bootstrap pattern.

Readers should leave able to decide when portability and predictable hot paths favor Variant, when uncontrolled recurring schemas and log-specific search favor CLP, and which capabilities are current OSS, Uber production extensions, or future directions.

---

## Narrative Map and Budget

| Section | Editorial job | Target |
|---|---|---:|
| Opening | Separate the SQL value from physical storage; establish the prior post and historical boundary | 190–220 |
| 1. Variant landscape | Explain the SQL/table/file stack and unshredded-versus-shredded tradeoff | 250–285 |
| 2. CLP model | Explain what specialized storage contributes and when it matters | 350–390 |
| 3. Predicate binding | Compare the physical work behind exact, polymorphic, and cross-field predicates | 245–280 |
| 4. Pluggable architecture | Contrast the forked integration with the current external native connector | 290–330 + diagram |
| 5. Using the connector | Give the shortest operator path and one representative query | 140–170 |
| 6. Two-scale metadata | Separate candidate-split planning from archive-local binding | 225–260 |
| 7. Connector-author path | Generalize the implementation into four compact stages | 300–350 |
| 8. Conclusion | Separate current OSS, production evidence, and future direction; return to the Presto thesis | 220–250 |
| Acknowledgments | Credit implementation, infrastructure, review, and production application | 40–60 |

Each section must create the question answered by the next: landscape → storage model → query work → extension boundary → operator experience → metadata → reusable author guidance → decision.

---

## Prior Foundation

YScope's August 2025 post, [“Querying CLP-compressed logs with SQL via Presto”](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82), owns the original motivation, ERT/schema-tree/logtype introduction, typed-accessor design, UDF rewriting, split discovery, pushdown mechanics, and evaluation against conventional Parquet.

This article may define ERTs, the schema tree, and logtypes briefly because the Variant comparison requires them, and may include one compact current-connector query. It must not reproduce the earlier optimizer walkthrough, metadata-query generation, plan diagrams, or benchmarks. Cite the earlier post at the first CLP–Presto history discussion.

---

## Section Contracts

### Opening: SQL Abstraction Does Not Erase Storage Design

- Frame schema variability as an ownership problem: analytical platforms do not control evolving producers.
- Define CLP immediately as a native columnar format and search engine for semi-structured data, then cite the August 2025 post.
- State the history precisely: the earlier implementation executed CLP natively in Velox workers of a Presto fork and required changes in that fork and Velox.
- Introduce the present contrast: PrestoDB lacks end-to-end Variant, while CLP is available through an out-of-band Presto Native connector loaded as a shared library.
- End by promising both storage analysis and a reusable connector boundary, not by justifying why another blog exists.

### 1. Variant in the Lakehouse Landscape

- Separate the layers: the engine defines the dynamic SQL type; [Iceberg v3](https://iceberg.apache.org/spec/#variant-type) defines the table type; Parquet defines the binary [`metadata`/`value` encoding](https://parquet.apache.org/docs/file-format/types/variantencoding/) and optional [shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/).
- Use [Trino Variant](https://trino.io/docs/current/functions/variant.html) only as implementation precedent in the Presto lineage, not as a deployable PrestoDB feature or the comparison target.
- Describe unshredded Variant as a row-local native binary value: known paths are navigable without parsing text, but inner fields lack independent Parquet columns and statistics.
- Explain that shredding gives writer-selected paths typed columns, compression, projection, and statistics. The choice must be finalized before the file is written; adding paths requires rewriting existing files.
- Preserve type-polymorphism accuracy: one `typed_value` has one declared type, while other types remain in `value`. Promoting every path/type creates a wide, sparse physical union.
- Use the Parquet [overview](https://parquet.apache.org/blog/2026/02/27/variant-type-in-apache-parquet-for-semi-structured-data/) and Databricks [introduction](https://www.databricks.com/blog/introducing-variant-new-open-standard-semi-structured-data-apache-parquettm-delta-lake) as background; specifications support normative claims.

### 2. What the Connector Preserves: CLP's Native Columnar Model

- Open with a direct reference to YScope's [August 2025 post](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) for the full ERT, schema-tree, logtype, and original-integration explanation. State that this section is deliberately only the refresher required for the Variant comparison.
- Establish the physical contrast: Parquet Variant is value-first with optional selected-path shredding; CLP is column-first.
- Define an encoded record table (ERT): an immutable archive groups recurring key-and-type shapes, records them in an archive-local schema tree, and assigns every observed schema node an encoded column. Polymorphic paths map to separate type-homogeneous columns.
- Treat the schema tree as execution metadata. It resolves exact paths and typed cross-field value searches once per archive split; a convenience schema can expose bindings after ingestion without rewriting archives.
- Explain logtypes once: CLP separates recurring free-form text from typed variables, enabling encoded free-text search and log-specific compression below the document-field boundary.
- Give the decision rule explicitly. CLP fits unpredictable paths/types, recurring uncontrolled schemas, cross-field/free-text search, and compact searchable archives. Variant with selective shredding fits interoperable tables and predictable hot paths.
- State limitations: with a stable schema, CLP has no field-columnization advantage over typed or comprehensively shredded Parquet; with near-singleton shapes, recurring-shape locality fades.
- State the evidence boundary: earlier benchmarks used conventional Parquet and do not establish an ordering against Variant or Variant shredding.

### 3. How Predicates Bind to Different Storage

- Link the August 2025 walkthrough instead of repeating UDF rewriting or optimizer mechanics. Begin after pushdown and ask which physical data the reader touches.
- Keep one three-row table comparing exact named paths, polymorphic paths, and typed cross-field value search.
- Scope Variant cost precisely: an unshredded recursive predicate reads relevant `value` pages and inspects per-row binary structure, not unrelated columns or textual JSON.
- State current versus future CLP behavior: typed wildcard value search is current OSS; wildcard key discovery and general `{path, value}` projection are future directions.
- Close on logical complementarity: future PrestoDB Variant could expose `event VARIANT` over CLP, while constant paths still bind to CLP columns and complete events require reconstruction.

### 4. Making Specialized Storage Pluggable Without a Presto Fork

- Explicitly contrast the earlier native-in-fork integration with the current out-of-band worker library. Never call the earlier implementation a plugin or non-native.
- Show Java coordinator responsibilities—metadata, tables, splits, rewriting, and handle serialization—versus native worker responsibilities—registration, deserialization, CLP scanning, and columnar batches.
- Use one diagram: SQL client → Java coordinator plugin → native worker loader/CLP `.so` → CLP storage.
- Explain the two upstream hooks: [PR #27653](https://github.com/prestodb/presto/pull/27653) registers factories from dynamically loaded code; [PR #27652](https://github.com/prestodb/presto/pull/27652) routes connector-specific serialized values to native deserialization.
- End with the architectural takeaway: native execution moved out of the fork, not out of the worker.

### 5. Using the Connector

- Give the operator path only: ingest through the YScope CLP OSS package, install matched coordinator/worker artifacts, enable the worker loader, and configure `etc/catalog/clp.properties`.
- Explain that the [documented production path](https://docs.yscope.com/presto/connector/clp.html) reads the MySQL metadata populated by CLP ingestion; custom providers may replace it.
- State the current boundary: read-only access through the `default` schema.
- Include one compact query showing typed exact paths, typed wildcard value search, and whole-event reconstruction. Mark names as examples and refer mechanics to the earlier post.
- Link the [packaging flow](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/tools/build-packages/README.md); do not turn the section into an installation guide.

### 6. Metadata Before and Within Each Split

- Separate file-level planning from per-split metadata. The provider identifies candidate KV-IR files and archives; the selected archive then maps paths/types to encoded columns.
- Treat file-level metadata as complementary to CLP and analogous to Iceberg's coarse planning role.
- Label Uber's billion-file metastore and eligible Top-N early termination as production extensions, not current OSS. Distinguish pruning (cannot match) from early termination (cannot affect the ordered result).
- Contrast Parquet Variant's row-local representation and shredded-column statistics with CLP's complete archive-local path/type map, dictionaries, logtypes, and indexes.
- Keep typed wildcard value search current; keep key discovery, structural/semantic split summaries, and candidate narrowing future and unbenchmarked.

### 7. Starting an Out-of-Band Presto Native Connector from Scratch

Teach four general stages, using CLP as evidence rather than presenting a CLP API inventory:

1. **Define the contract:** tables, types, splits, pushdowns, residuals, and cross-boundary handles.
2. **Implement both halves:** Java plugin/factory/codecs plus native `registerExtensions`, Presto-to-Velox adapter, version script, and `plugin.dir`. Mention `use-connector-provided-serialization-codecs`.
3. **Release a compatible bundle:** separate JAR and `.so`, adjacent dependency `lib/`, `$ORIGIN/lib`, manylinux baseline, package/tar/init-container delivery, and complete CPU/Presto/Velox compatibility. Retain the Folly `F14LinkCheck`/Velox `get_cxx_flags` lesson.
4. **Test the shipped boundary:** query-runner tests for Java planning and serialization, standalone tests for dependency-free native helpers, and a real worker for plugin-bound behavior. Use cumulative [PR #64](https://github.com/y-scope/clp-plugin-presto-connector/pull/64) and `task integration-tests:run` as the current example.

Keep only a publication note to replace PR #64 with stable merged documentation and confirm the release matrix.

### 8. One Presto, Different Storage Contracts

- Use an evidence ladder: current OSS connector capabilities → approved Uber production scale → clearly labeled future directions.
- Do not conclude that CLP has defeated or survived Variant. Conclude that Variant offers a portable logical/table contract, CLP offers specialized physical behavior, and Presto can support both through connector boundaries.
- Preferred final idea: one SQL engine can support different storage contracts without embedding every storage engine in the Presto codebase.

### Acknowledgments

Credit YScope engineers for leading connector implementation; IBM engineers and the Presto community for infrastructure and review; and YScope and Uber engineers for extending and applying the architecture in production.

---

## Information Design

Use only three aids: the predicate table in Section 3, the coordinator/worker diagram in Section 4, and the compact query in Section 5. Do not add a benchmark dashboard, second layout diagram, full configuration block, optimizer walkthrough, or exhaustive API inventory.

---

## Evidence Labels

| Label | Meaning | Examples |
|---|---|---|
| **Prior foundation** | Established by the August 2025 post or earlier CLP research | ERTs, logtypes, typed accessors, rewriting, pushdown, conventional-Parquet evaluation |
| **Current OSS** | Verifiable in current public source | KV-IR/archive and mixed scans, typed exact paths, wildcard value search, whole-event reconstruction, pluggable metadata |
| **Uber production** | Deployed behavior not necessarily public | Scale figures, advanced metastore, early termination |
| **Future direction** | Architecturally compatible but not promised | `event VARIANT`, key discovery, wildcard projection, structural/semantic summaries |
| **Comparative claim** | Requires a controlled evaluation | Compression or query-performance ordering versus Parquet Variant |

---

## Non-Negotiable Safeguards

- Keep the CLP connector primary and Variant supportive. The article is neither a Variant rebuttal nor a connector advertisement.
- Avoid universal or sales language such as “superior” or “best”; every advantage must name the workload condition that creates it and the condition under which it fades.
- State the availability asymmetry: CLP works in PrestoDB through the external connector; PrestoDB does not yet support Variant end to end.
- Distinguish logical complementarity from physical equivalence. Future Variant SQL could sit over CLP without changing CLP storage.
- Describe the earlier implementation as native execution in Velox workers of a Presto fork requiring fork/Velox changes. Never call it an external plugin or non-native.
- Describe the current worker component as an out-of-band shared library loaded by Presto Native.
- Call unshredded Variant a native binary row-local value, not textual JSON, an opaque blob, a row store, or literal “row compression.”
- Do not claim Variant lacks pushdown, cannot express recursive predicates, or cannot preserve type-polymorphic values. Scope exhaustive scans to relevant `value` pages and per-row binary structure.
- CLP is intrinsically columnized; “all fields shredded” is only an analogy. Do not claim a field-columnization advantage over typed/comprehensively shredded Parquet for stable schemas.
- Tie CLP advantages to recurring schemas, repetitive log text, unpredictable fields/types, or cross-field search. Coalescing near-singleton ERTs controls overhead but cannot create structural repetition.
- Never apply conventional-Parquet benchmark results to Variant or shredding. A direct ordering requires the same corpus, codec, shredding policy, query set, and reader versions.
- Separate current typed wildcard value search from future key discovery and general wildcard projection.
- Separate current OSS, Uber production, and future metadata behavior. Keep semantic summaries future; distinguish pruning from early termination.
- Keep searchable KV-IR, immutable archives, and mixed scans distinct. ERT and archive-local schema-tree mechanics describe the immutable archive path.
- Qualify “build once, run everywhere” with libc, architecture, dependency, Presto Native/Velox, and target-CPU compatibility. Folly's `F14LinkCheck` makes CPU flags part of the loading contract.
- Divide testing by runtime dependency: pure native helpers may be unit-tested; plugin-bound code whose symbols come from the worker needs real-worker integration.
- Use production figures only as evidence of operation at scale. Preserve YScope, IBM/Presto-community, and Uber attribution.
- Do not add the Pinot analogy or Uber's broader hot-tier architecture; neither advances the Presto connector thesis.

---

## Verify Before Publication

- Variant chronology: Iceberg v3 adoption, Parquet Variant/shredding approval dates, and current Trino support status.
- Current PrestoDB Variant status across Java, serialization, Presto Native/Velox, Iceberg, and Parquet reading.
- Exact historical/current connector boundary and the merge state/capabilities of Presto PRs #27653 and #27652.
- Current public CLP functions and semantics; confirm key discovery/general wildcard projection remain future.
- “One of the first” versus “the first” independently developed pluggable Presto Native connector.
- Final release matrix: Presto/Velox pins, images, libc, architectures, CPU targets, and `F14LinkCheck` mode.
- Merge and stable documentation for cumulative connector PR #64; replace the draft's PR link.
- Approval for Uber figures, worker count, metadata claims, acknowledgments, and future-direction wording.

---

## Out of Scope

- A full Variant, Iceberg v3, Trino, CLP-format, or μSlope tutorial.
- Reproduction of the earlier optimizer, split-discovery, pushdown, or benchmark walkthrough.
- A conventional-Parquet benchmark presented as evidence about Variant.
- Uber's complete ingestion or hot-tier architecture.
- A full SQL API, installation, build-troubleshooting, or configuration guide.
- Unverified performance multipliers, semantic-search costs, or product commitments.
