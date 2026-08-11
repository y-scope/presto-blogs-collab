# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

- **Status:** Editorial outline synchronized with the current draft
- **Audience:** Presto users, operators, maintainers, and external connector authors
- **Target:** 2,150–2,300 words
- **Primary subject:** The current pluggable CLP connector for Presto Native
- **Supporting background:** Variant as the standardized lakehouse reference for semi-structured data
- **Point of view:** Presto users and maintainers deciding when specialized storage belongs behind a common SQL interface

---

## Editorial Contract

The article explains how Presto can query a specialized native semi-structured format without embedding it in a fork. Variant supplies the broader lakehouse context. It is not a currently available PrestoDB alternative or a co-equal subject.

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
| Opening | Separate the SQL value from physical storage, then establish the prior post and historical boundary | 190–220 |
| 1. Variant landscape | Explain the SQL/table/file stack and unshredded-versus-shredded tradeoff | 210–235 |
| 2. CLP model | Explain what specialized storage contributes and when it matters | 320–350 |
| 3. Predicate binding | Compare the physical work behind exact, polymorphic, and cross-field predicates | 200–225 |
| 4. Metadata beyond pruning | Separate metastore planning from archive-local narrowing | 235–250 |
| 5. Deployment to first query | Show the operator outcome through installation references and one representative query | 190–220 |
| 6. Pluggable connector reference | Explain the Java/native contract and turn the CLP implementation into reusable author guidance | 450–480 + diagram |
| 7. Conclusion | Turn the comparison into a workload-based storage decision, then return to the Presto thesis | 210–235 |
| Acknowledgments | Credit implementation, infrastructure, review, and production application | 35–50 |

Each section must create the question answered by the next: landscape → storage model → query work → metadata → operator experience → reusable connector architecture → decision.

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

- Separate the layers. The engine defines the dynamic SQL type. [Iceberg v3](https://iceberg.apache.org/spec/#variant-type) defines the table type. Parquet defines the binary [`metadata`/`value` encoding](https://parquet.apache.org/docs/file-format/types/variantencoding/) and optional [shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/).
- Use [Trino Variant](https://trino.io/docs/current/functions/variant.html) only as implementation precedent in the Presto lineage, not as a deployable PrestoDB feature or the comparison target.
- Describe unshredded Variant as a row-local native binary value: known paths are navigable without parsing text, but inner fields lack independent Parquet columns and statistics.
- Explain that shredding gives writer-selected paths typed columns, compression, projection, and statistics. The choice must be finalized before the file is written. Adding paths requires rewriting existing files.
- Preserve type-polymorphism accuracy: one `typed_value` has one declared type, while other types remain in `value`. Promoting every path/type creates a wide, sparse physical union.
- Clarify that even fully shredding all observed paths retains one file-wide schema. Fields absent from a row become missing optional-column entries. Parquet encodes this sparsity compactly through definition levels rather than literal null payloads.
- Use the Parquet [overview](https://parquet.apache.org/blog/2026/02/27/variant-type-in-apache-parquet-for-semi-structured-data/) and Databricks [introduction](https://www.databricks.com/blog/introducing-variant-new-open-standard-semi-structured-data-apache-parquettm-delta-lake) as background. Specifications support normative claims.

### 2. CLP's Distinctive Storage Model: Native Columnar Semi-Structured Data

- Open with a direct reference to YScope's [August 2025 post](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) for the full ERT, schema-tree, logtype, and original-integration explanation. State that this section is deliberately only the refresher required for the Variant comparison.
- Establish the physical contrast with a table-within-a-file analogy. A Parquet file remains one table whose rows contain Variant values. Optional shredding adds writer-selected typed columns to that table. A CLP archive is closer to a container of nested columnar tables.
- Define an encoded record table (ERT): each ERT groups one recurring key-and-type shape into type-homogeneous encoded columns. The archive-local schema tree relates ERTs and maps every observed path/type to their columns, including separate columns for polymorphic types. ERTs are internal storage structures, not separately addressable SQL tables.
- Contrast structural absence with explicit nulls. Each ERT contains only the fields in its shape, avoiding missing placeholders across unrelated shapes. Explicit source null values are still preserved.
- Treat the schema tree as archive-local execution metadata rather than a global table schema. A convenience schema can expose common SQL bindings without rewriting archives. Leave the detailed path-binding work to Section 3.
- Explain logtypes once: CLP separates recurring free-form text from typed variables, enabling encoded free-text search and log-specific compression below the document-field boundary. Connect shape and logtype locality to query work: structural predicates narrow scans to matching ERTs/columns, while text predicates search encoded logtypes/variables.
- Give the decision rule explicitly. CLP fits unpredictable paths/types, recurring uncontrolled schemas, cross-field/free-text search, and compact searchable archives. Variant with selective shredding fits interoperable tables and predictable hot paths.
- State limitations. With a stable schema, CLP has no field-columnization advantage over typed or comprehensively shredded Parquet. With near-singleton shapes, recurring-shape locality fades.
- State the evidence boundary: earlier benchmarks used conventional Parquet and do not establish an ordering against Variant or Variant shredding.

### 3. From Predicate to Physical Scan: Parquet Variant and CLP

- Link the August 2025 walkthrough instead of repeating UDF rewriting or optimizer mechanics. Begin after pushdown and ask which physical data the reader touches.
- Keep one three-row table comparing exact named paths, polymorphic paths, and typed cross-field value search.
- Scope Variant cost precisely: an unshredded recursive predicate reads relevant `value` pages and inspects per-row binary structure, not unrelated columns or textual JSON.
- State current CLP behavior plainly: when a query uses a wildcard rather than an exact key, the connector infers the value type and resolves compatible encoded columns from split metadata. Keep returning matched keys and general `{path, value}` projection as separate future directions.
- Close by introducing the two metadata layers behind those scans: cross-file metastore planning and metadata inside each selected CLP archive. Leave future Variant complementarity to the conclusion.

### 4. Beyond File Pruning: How Metastore Planning and CLP Archive Metadata Narrow the Search

- Follow one sequence: general metastore planning → workload-driven early termination → CLP archive metadata.
- Begin with general file-level planning. A provider identifies candidate KV-IR files and archives using time bounds, dimensions, statistics, and related metadata before worker splits are created. This capability is complementary to CLP and common across analytical metastores.
- Give Uber's billion-file internal-metastore scale and resource efficiency qualitatively. Do not publish its infrastructure footprint or present it as the OSS reference provider.
- Then present early termination as a reusable specialization when files have trustworthy beginning and ending timestamps and newest-first limited queries are common. Time-sorted files and append-only publication are helpful, not defining requirements. Identify Uber as the production example rather than implying that the idea applies only at Uber. Exact predicate-specific matching counts and the required time-bound proof are not normally part of a general metastore interface.
- Label Uber's billion-file metastore and current eligible Top-N implementation as production extensions, not current OSS. State the correctness invariant in four parts: exact per-file matching counts for metadata-resolvable predicates, descending maximum-timestamp order, a watermark at the earliest timestamp covered and frozen when the count first reaches `N`, and continued selection of every file overlapping that cutoff. Stop only when the next file is strictly older. Unsupported row-level predicates disable the guarantee. Distinguish pruning (cannot match) from safe early termination (remaining files cannot affect the ordered result).
- End with metadata inside the selected CLP archive. Treat ordinary column statistics and indexes as a shared baseline rather than an either/or contrast. What distinguishes CLP is the additional complete archive-local path/type-to-column map, dictionaries, logtypes, and indexes that are not part of the standard Parquet Variant representation.
- State the ownership boundary directly. Metastore metadata determines which files become splits. CLP archive metadata narrows the work inside a selected split.
- Avoid repeating the predicate-binding comparison from Section 3. Focus on how a worker initializes from compact archive metadata and streams candidate columns without local staging. Keep structural/logtype summaries future and unbenchmarked.

### 5. From Deployment to First Query with the CLP Connector

- Give the operator path only: ingest through the YScope CLP OSS package, install matched coordinator/worker artifacts, enable the worker loader, and configure `etc/catalog/clp.properties`.
- Explain that the [documented production path](https://docs.yscope.com/presto/connector/clp.html) reads the MySQL metadata populated by CLP ingestion. Custom providers may replace it.
- State the current boundary: read-only access through the `default` schema.
- Include one compact query showing typed exact paths, typed wildcard value search, and whole-event reconstruction. Mark names as examples and refer mechanics to the earlier post.
- Link the [packaging flow](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/tools/build-packages/README.md). Do not turn the section into an installation guide.
- Link the connector's [Compose stack](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/integration-tests/docker-compose.yaml) as the concrete matched-artifact and init-container example. Link the CLP [deployment Compose stack](https://github.com/jackluo923/clp/blob/cf9e2fab8e307a90a1009364c7a21d7a7b0a1727/tools/deployment/presto-clp/docker-compose.yaml) as the complete coordinator/worker deployment example.
- End by revealing that the short operator path depends on a two-part coordinator–worker contract.

### 6. Building a Pluggable Presto Native Connector: CLP as a Reference

- Explicitly contrast the earlier native-in-fork integration with the current out-of-band worker library. Never call the earlier implementation a plugin or non-native.
- Show Java coordinator responsibilities—metadata, tables, splits, rewriting, and handle serialization—versus native worker responsibilities—registration, deserialization, CLP scanning, and columnar batches.
- Use one diagram: SQL client → Java coordinator plugin → native worker loader/CLP `.so` → CLP storage.
- Explain the two upstream hooks. [PR #27653](https://github.com/prestodb/presto/pull/27653) registers factories from dynamically loaded code. [PR #27652](https://github.com/prestodb/presto/pull/27652) routes connector-specific serialized values to native deserialization.
- Move from architecture to three reusable stages without restarting the story:
  1. **Define and implement the contract:** tables, types, splits, pushdowns, residuals, and cross-boundary handles. Cover the Java plugin/factory/codecs plus native `registerExtensions`, adapter, version script, and `plugin.dir`. Mention `use-connector-provided-serialization-codecs`.
  2. **Package one compatible unit:** separate JAR and `.so`, adjacent dependency `lib/`, `$ORIGIN/lib`, manylinux baseline, package/tar/init-container delivery, and complete CPU/Presto/Velox compatibility. Retain the Folly `F14LinkCheck`/Velox `get_cxx_flags` lesson.
  3. **Test the deployed boundary:** query-runner tests for Java planning and serialization, standalone tests for dependency-free native helpers, and a real worker for plugin-bound behavior. Use cumulative [PR #64](https://github.com/y-scope/clp-plugin-presto-connector/pull/64) and `task integration-tests:run` as the current example.
- State the historical takeaway once near the beginning: native execution moved out of the fork, not out of the worker. End on the deployable unit: coordinator artifact, native bundle, and serialized protocol together.

Keep the publication check in this outline rather than the reader-facing draft. Replace PR #64 with stable merged documentation and confirm the release matrix.

### 7. Conclusion: Choosing a Storage Model for Semi-Structured Data in Presto

- Give the decision rule once. Variant fits cross-engine portability with predictable paths for shredding. CLP fits when the analytical platform cannot control upstream schemas or content or preselect useful query paths.
- Connect recurring structural patterns and static text interleaved with variable values to ERT/logtype compression. Connect archive metadata to narrower lookups. Retain the stable-schema and near-unique-shape limits.
- Use one compact evidence paragraph: representative current OSS capabilities followed by approved Uber production scale. Keep advanced planning and early termination labeled as Uber extensions.
- State the future logical/physical distinction concretely. One Variant SQL interface would not make scans alike: Parquet reads `value` or shredded columns, while CLP binds paths to ERT columns and encoded text streams.
- End on the Presto contribution. The pluggable native boundary provides common SQL and planning while keeping storage-specific execution outside the Presto codebase.

### Acknowledgments

Credit YScope engineers for leading connector implementation, IBM engineers and the Presto community for infrastructure and review, and YScope and Uber engineers for extending and applying the architecture in production.

---

## Information Design

Use only three aids: the predicate table in Section 3, the compact query in Section 5, and the coordinator/worker diagram in Section 6. Do not add a benchmark dashboard, second layout diagram, full configuration block, optimizer walkthrough, or exhaustive API inventory.

---

## Evidence Labels

| Label | Meaning | Examples |
|---|---|---|
| **Prior foundation** | Established by the August 2025 post or earlier CLP research | ERTs, logtypes, typed accessors, rewriting, pushdown, conventional-Parquet evaluation |
| **Current OSS** | Verifiable in current public source | KV-IR/archive and mixed scans, typed exact paths, wildcard value search, whole-event reconstruction, pluggable metadata |
| **Uber production** | Deployed behavior not necessarily public | Scale figures, advanced metastore, early termination |
| **Future direction** | Architecturally compatible but not promised | `event VARIANT`, returning matched keys, wildcard projection, structural/semantic summaries |
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
- Never apply conventional-Parquet benchmark results to Variant or shredding. A direct ordering requires the same corpus, codec, shredding policy, query set, and reader versions.
- Separate current typed value search over wildcard keys from future return of matched keys and general wildcard projection.
- Separate current OSS, Uber production, and future metadata behavior. Keep semantic summaries future. Distinguish pruning from early termination.
- Keep searchable KV-IR, immutable archives, and mixed scans distinct. ERT and archive-local schema-tree mechanics describe the immutable archive path.
- Qualify “build once, run everywhere” with libc, architecture, dependency, Presto Native/Velox, and target-CPU compatibility. Folly's `F14LinkCheck` makes CPU flags part of the loading contract.
- Divide testing by runtime dependency. Pure native helpers may be unit-tested. Plugin-bound code whose symbols come from the worker needs real-worker integration.
- Use production figures only as evidence of operation at scale. Preserve YScope, IBM/Presto-community, and Uber attribution.
- Do not add the Pinot analogy or Uber's broader hot-tier architecture. Neither advances the Presto connector thesis.

---

## Verify Before Publication

- Variant chronology: Iceberg v3 adoption, Parquet Variant/shredding approval dates, and current Trino support status.
- Current PrestoDB Variant status across Java, serialization, Presto Native/Velox, Iceberg, and Parquet reading.
- Exact historical/current connector boundary and the merge state/capabilities of Presto PRs #27653 and #27652.
- Confirm current public CLP functions and semantics. Confirm that returning matched keys and general wildcard projection remain future.
- “One of the first” versus “the first” independently developed pluggable Presto Native connector.
- Final release matrix: Presto/Velox pins, images, libc, architectures, CPU targets, and `F14LinkCheck` mode.
- Confirm the merge and stable documentation for cumulative connector PR #64. Replace the draft's PR link.
- After the deployment PRs land, verify both Compose links and replace the pinned `jackluo923/clp` URL with the canonical `y-scope/clp` main-branch path.
- Approval for Uber figures, worker count, metadata claims, acknowledgments, and future-direction wording.

---

## Out of Scope

- A full Variant, Iceberg v3, Trino, CLP-format, or μSlope tutorial.
- Reproduction of the earlier optimizer, split-discovery, pushdown, or benchmark walkthrough.
- A conventional-Parquet benchmark presented as evidence about Variant.
- Uber's complete ingestion or hot-tier architecture.
- A full SQL API, installation, build-troubleshooting, or configuration guide.
- Unverified performance multipliers, semantic-search costs, or product commitments.
