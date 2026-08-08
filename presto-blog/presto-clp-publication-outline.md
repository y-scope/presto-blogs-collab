# Petabyte-Scale Schema-Less Data, Columnar by Design: The CLP Connector for Presto Native

- **Status:** Editorial strategy outline synchronized with the current draft
- **Audience:** SQL users with basic Presto familiarity; storage-format and connector terms should be explained briefly or linked at first use
- **Article target:** 1,900–2,300 words
- **Primary goal:** Explain why CLP is a strong fit for recurring but uncontrolled schema-less data, and how the Presto Native connector preserves its storage advantages through planning and pushdown
- **Secondary goal:** Provide a reference architecture for other independently developed Presto Native connectors

---

## Intended Audience and Reading Level

### Primary audience

- Presto users, operators, and data-platform engineers evaluating how to store and query evolving semi-structured data.
- Readers may work with service logs, traces, audit records, AI coding-agent session histories, or other producer-owned records; the article is not written only for log-platform specialists.

### Secondary audience

- Presto contributors and connector authors interested in the external Presto Native connector path.
- Storage-engine developers evaluating how query planning and pushdown can preserve a native format's advantages.

### Assumed knowledge

- Familiarity with SQL tables, columns, `SELECT`, `WHERE`, `ORDER BY`, and `LIMIT`.
- Basic awareness that Presto queries external data through catalogs and connectors.

### Not assumed

- Parquet row-group or Variant internals.
- Shredding, ERTs, logtypes, pushdown, splits, Presto Native plugin loading, or connector serialization.

The main argument must remain understandable without opening any link. Define each non-obvious term briefly at first use; link official documentation or source code for readers who want implementation depth.

---

## Editorial Strategy

The article answers one operational question:

> How can Presto query data whose producers control the schema without giving up field-level columnar execution or predicting every useful path before ingestion?

The argument is causal, not a feature inventory:

1. Producers—not the analytical table owner—control event structure.
2. Iceberg Variant can preserve dynamic values, but Parquet must either keep inner fields co-located or shred paths selected at write time.
3. CLP discovers recurring key-and-type shapes and columnizes every observed schema node, so it needs no shredding policy.
4. Query-plan resolution and pushdown bind logical paths to those existing encoded columns.
5. An in-process Presto Native connector dispatches searchable KV-IR and immutable archive splits, preserving storage-native execution for each format.
6. Metadata, schema resolution, dictionaries, sequential object-store access, and early termination avoid work at progressively finer levels.

The comparison must remain bounded. With one stable schema, CLP and typed Iceberg/Parquet converge at field columnization, although CLP may still benefit from logtype encoding. Variant remains valuable for interoperability and dynamic values. CLP's strongest structural case is many recurring but uncontrolled schemas and unpredictable future paths and types; wildcard value search is a current complementary capability, while key discovery remains future work.

### Narrative map

Each section has an explicit editorial contract: why it is necessary, why its material is presented in that form, what the intended audience gains, and how it advances the larger argument.

| Section | Reader question | Why needed | Audience benefit | Story contribution |
|---|---|---|---|---|
| Opening | Who owns the schema? | Establish the operational constraint before formats | Readers distinguish uncontrolled producer change from ordinary schema evolution | Creates the need for flexible storage with columnar execution |
| 1. Iceberg Variant | Can't I just use Iceberg? | Address the strongest conventional alternative | Readers can make a fair workload-dependent choice | Defines the precise storage tradeoff CLP changes |
| 2. CLP storage | What does CLP do differently? | Explain the central physical model | Readers understand ERTs, logtypes, and late binding | Supplies the mechanism behind the article's main claim |
| 3. Query execution | Does SQL reach those columns? | Prove storage advantages survive the query layer | Readers can trace SQL through pushdown to encoded execution | Connects the physical model to actual Presto behavior |
| 4. Architecture | How does CLP fit into Presto Native? | Expand from one scan to the complete integration | Connector users and authors understand system boundaries | Shows why storage-native execution is preserved end to end |
| 5. Work avoidance | Why should this scale? | Replace vague performance language with mechanisms | Operators see which work and object-store costs are avoided | Turns architecture into a credible scale argument |
| 6. Reuse | Why does this matter beyond CLP? | Generalize the completed integration | Connector authors receive a practical lifecycle checklist | Broadens the article from product story to community reference |
| 7. Proof and future | Is it real, and what comes next? | Close with evidence, boundaries, and direction | Readers distinguish usable OSS, production proof, and future work | Converts the argument into confidence and a memorable conclusion |

### Presentation rules

- Lead each section with its answer, then explain the mechanism.
- Introduce one layer at a time: ownership → physical layout → query binding → architecture → scale.
- Compare like with like: SQL type, table type, physical encoding, reader behavior, and storage engine are different layers.
- Prefer causal language—“because every path is already columnized”—over lists of features.
- Mark capabilities as **current OSS**, **Uber production**, or **future direction** at first mention.

### Section admission test

Before retaining a section, the editorial plan must answer:

1. What reader question or argument gap would remain if this section were removed?
2. Why is this material presented as an example, comparison, mechanism, diagram, or proof at this point in the article?
3. Which intended reader benefits, and what can that reader understand or decide afterward?
4. What claim, transition, or conclusion does the section enable next?

Material that cannot answer all four belongs in the knowledge base, documentation links, or out-of-scope list rather than the article.

---

## Title Strategy

**Preferred:** **Petabyte-Scale Schema-Less Data, Columnar by Design: The CLP Connector for Presto Native**

This title pairs the approved production outcome—approximately 25 PB of raw logs per month at Uber—with CLP's differentiating capability. “Columnar by design” means that CLP makes observed structure part of its storage model rather than providing columnar access only for paths selected into a separate shredding layout. It expresses the native-columnar idea without repeating “Native,” which remains in the Presto Native product name.

**Conceptual alternate:** **Schema-Less, Yet Columnar at Petabyte Scale: The CLP Connector for Presto Native**. Use this when a shorter, more approachable conceptual hook matters more than stating “native columnar access” precisely.

---

## Opening: The Schema Is Not Owned by the Table

**Target:** 190–230 words

### Editorial necessity

Show that this is an ownership problem, not merely a missing `ALTER TABLE`. Independent producers can add fields, omit them, change nesting, or change types without coordinating with the downstream table owner.

### Presentation strategy

- Open with the contrast between a Parquet writer that knows its schema and a downstream analytics platform that receives semi-structured records after producers have changed them.
- Contrast coordinated table-schema evolution with per-event polymorphism.
- Establish three requirements: preserve the long tail, keep common fields convenient, and delay path-selection decisions until investigation time.
- Introduce CLP only at the end as the answer: discover structure, columnize it, and let Presto bind SQL paths to native columns.

### Required content

- Service logs are one example, not the boundary. Include traces, audit records, and AI coding-agent session histories—execution logs that mix tool calls, structured state, and natural-language output.
- State that logs provide the production evidence in this article while the storage problem is broader.
- Schema-less does not mean structure-less or necessarily textual JSON.
- Establish the problem before the proof, then use one sentence at the end of the opening to substantiate the petabyte-scale title. Keep the detailed deployment figures for Section 7.

### Audience benefit and story contribution

- **Audience benefit:** Readers recognize their own producer/platform ownership mismatch and understand why ordinary schema evolution is insufficient.
- **Story contribution:** Establishes the need for late-bound schema-less access with columnar execution, making the Iceberg objection the natural next step.

---

## 1. Why Not Just Use Iceberg Variant?

**Target:** 240–280 words

### Editorial necessity

Answer the reader's strongest objection before presenting CLP. This keeps the comparison fair and makes the actual differentiation obvious.

### Presentation strategy

- Start with **yes**: where supported, Iceberg v3 Variant can preserve schema-less records and may be right for interoperability or predictable hot paths.
- Separate Trino `VARIANT` as a SQL type, Iceberg `variant` as a table type, and Parquet Variant as physical `metadata`/`value` encoding with optional shredding.
- Describe unshredded Variant as a **native binary document column**: typed and directly navigable, but with inner fields physically co-located.
- Describe shredding as writer-selected physical promotion that gives chosen paths typed columns, encodings, statistics, and data skipping.
- Close with the workload boundary: for one stable schema, CLP has no field-columnization advantage over typed Iceberg/Parquet; recurring uncontrolled schemas and unpredictable paths are CLP's stronger structural case. Treat logtype encoding as a separate axis.

### Required content

- Unshredded Variant helps avoid materializing unrelated values; shredding additionally helps avoid reading or scanning unrelated encoded data for selected paths.
- Shredding every observed path would require a per-file union of heterogeneous paths and types. Absent fields are missing, creating a wide, sparse physical schema; Parquet encodes missingness compactly, but promoted paths still carry schema, column, and metadata overhead.
- The Variant remainder is the escape hatch for the dynamic long tail, which is why selective shredding is a practical compromise rather than simply “shred everything.”
- Shredding is physical, write-time, and file-local. Changing existing files requires rewriting or replacing them; a catalog change cannot create inner columns retroactively.
- Do not call Variant an opaque blob, claim it lacks pushdown, or say Iceberg cannot store schema-less data.

> Iceberg can store schema-less data; CLP removes the need to predict which paths deserve columnar storage.

> Parquet chooses physical path independence at write time and exploits that choice at query time.

### Audience benefit and story contribution

- **Audience benefit:** Readers can decide when Iceberg/Parquet is sufficient and can separate dynamic-value representation from physical path independence.
- **Story contribution:** Defines the write-time promotion problem that CLP's intrinsic columnization resolves next.

---

## 2. CLP Needs No Shredding Mode

**Target:** 350–410 words

### Editorial necessity

This is the central storage insight. It must make “no shredding mode” read as a consequence of intrinsic columnization, not a missing capability.

### Presentation strategy

Move from physical structure to user experience:

1. Build an archive-level schema tree and store each observed key-and-type schema once.
2. Group records with the same schema into an encoded record table (ERT).
3. Give every schema node a native encoded ERT column.
4. Explain why the purpose-built archive holds many ERTs rather than forcing them into one Parquet file schema or one file per schema.
5. Separate recurring logtype text from changing variables inside natural-language values.
6. Compare stable, recurring-uncontrolled, and near-singleton schema regimes.
7. Explain the convenience schema as late-bound SQL exposure over storage that is already columnized.

### Required content

- ERTs are compact homogeneous tables, not one globally sparse union of all fields.
- Grouping by schema is what lets CLP columnize every observed path without creating the wide sparse layout that global “shred everything” would imply.
- The same grouping contributes to compression: each ERT column contains values with the same structural role and type, giving specialized encoders homogeneous input. Present this as one reason for an advantage on heterogeneous logs, alongside type-specific encodings and logtype/variable separation.
- Compare physical compression with Parquet or Parquet-backed Iceberg, not with Iceberg in isolation; Iceberg is the table format rather than the value codec.
- A Parquet file repeats one physical schema across its row groups. One union schema recreates sparsity; one Parquet file/table per observed schema can turn schema diversity into small-object, repeated-metadata, discovery, and planning overhead.
- A CLP archive packages many ERTs behind one schema tree together with dictionaries, indexes, logtypes, and encoded streams. This purpose-built multi-table unit is a core format rationale, alongside CLP's specialized compression and search encodings.
- Present this as a tradeoff, not an impossibility claim: a Parquet dataset can contain many files and schemas, but that makes the dataset/catalog layer manage what CLP represents inside one searchable archive.
- One logical path may map to multiple ERT columns across schemas or type variants.
- “All fields are shredded” is only an analogy; CLP has no Variant remainder, promotion list, or shredding operation.
- Use the conceptual template `Request <int> failed after <int> ms` to show both logtype/variable separation and type awareness. State that the typed placeholders are explanatory notation, while CLP records variable types and applies suitable encodings. This is syntactic pattern extraction, not natural-language understanding.
- Link the broader compression evidence to μSlope, but require a same-corpus, codec- and shredding-controlled comparison before publishing a Parquet multiplier.

Workload regimes:

1. **One stable schema:** typed Parquet and one ERT converge at path columnization; CLP may still benefit from logtype/variable encoding.
2. **Many recurring, uncontrolled schemas:** CLP's strongest expected relative case because every observed node is columnized without a global schema or promotion list.
3. **Almost one schema per event:** neither design can amortize a recurring record schema. CLP coalesces small ERTs internally instead of retaining one physical table per schema, controlling singleton overhead; their structural layouts converge in this respect, although CLP may still find repeated logtypes inside string values.

The important measure is records per schema, not raw schema count. Different values do not imply different schemas.

- A convenience schema can expose selected path-and-type bindings as ordinary typed columns after write time without rewriting CLP archives.
- Path/type combinations outside that view remain accessible and map to existing ERT columns.
- Keep future `event VARIANT` API details in Section 3; this section establishes that storage does not depend on SQL exposure.
- Do not claim perfect byte isolation or universal superiority over Parquet.

> CLP needs no shredding mode because every observed path is columnized during ingestion.

> **One logical document, many native physical columns.**

> The convenience schema is late-bound projection over already-columnized storage.

### Audience benefit and story contribution

- **Audience benefit:** Readers can explain ERTs, why CLP uses a purpose-built multi-table archive, its favorable workload, and why post-write convenience-schema changes do not alter files.
- **Story contribution:** Establishes both the intrinsic-columnization and format-design rationale that Section 3 must prove is reachable from SQL.

---

## 3. From SQL Expression to Encoded Column

**Target:** 410–470 words, including one SQL example and one comparison table

### Editorial necessity

Intrinsic columnization matters only if the query layer reaches it. This section proves the end-to-end path and separates current public behavior from future Variant ergonomics.

### Presentation strategy

Move from concrete syntax to execution, then future UX:

1. Explain the current PrestoDB type-system gap.
2. Show the public CLP functions in one realistic query.
3. Reveal that these are planner markers, not row-at-a-time JSON functions.
4. Walk through the four execution phases.
5. Compare what the same phases bind to for Iceberg/Parquet and CLP.
6. Introduce `event VARIANT` as a future SQL bridge over unchanged CLP storage.
7. Finish with current wildcard value search versus future wildcard key discovery/projection.

### Required content

**Current OSS:**

- PrestoDB has `JSON`, `ROW`, and homogeneous `ARRAY(T)`, but no first-class `VARIANT` in the referenced release.
- Show representative exact-path accessors such as `CLP_GET_STRING` and `CLP_GET_BIGINT`; link the public source for the full function list.
- `CLP_GET_JSON_STRING()` reconstructs the complete event.
- Typed `CLP_WILDCARD_*_COLUMN()` functions search values across fields whose names are not enumerated.
- Typed accessors adapt to PrestoDB's planned result types; they are not a CLP storage limitation.

Use one query combining conventional columns, exact-path projection/filtering, wildcard value search, whole-event reconstruction, newest-first ordering, and a limit.

Use this mechanism line exactly:

> **Query-plan resolution → projection/predicate pushdown → split-local schema resolution → native encoded execution**

- Resolution retains constant paths, requested types, and compatible operations.
- Pushdown carries that intent in the scan instead of evaluating generic JSON per record.
- Each worker matches paths against its archive's schema tree and binds relevant ERT columns.
- CLP searches/projects encoded columns; Presto evaluates unsupported residual expressions.

Use a four-row comparison table:

| Phase | Iceberg/Parquet Variant | CLP |
|---|---|---|
| Plan | Resolve Variant paths and filters | Retain CLP paths, types, and operations |
| Pushdown | Pass supported subfields and filters to the file reader | Pass them to the CLP scan |
| Bind | Use a shredded column if present; otherwise navigate Variant values | Match the split's schema tree to existing ERT columns |
| Scan | Read shredded columns or pages containing unshredded values | Search and project encoded ERT columns |

**Future direction:**

- With Variant across the PrestoDB coordinator, serialization, Presto Native/Velox types, and operators, expose one logical `event VARIANT` without changing CLP storage.
- Constant expressions such as `event['request']['id']` must use the same resolution and pushdown path before the Variant surface is release-ready; bare `event` reconstructs the complete value.
- Current wildcard functions search values across unknown fields. Future schema-tree-backed key discovery and `{path, value}` projection are separate operations.
- Unshredded Variant must inspect relevant per-value object metadata for arbitrary key patterns; CLP can resolve candidate paths against archive schema metadata before record scanning.
- Do not claim current wildcard-key projection or explain another engine's API choices from storage alone.

### Audience benefit and story contribution

- **Audience benefit:** Readers can trace a logical path from SQL to encoded columns and distinguish today's API from future Variant ergonomics.
- **Story contribution:** Proves the storage advantage survives planning and pushdown, then creates the need to explain the native integration boundary.

---

## 4. A Native Storage Engine Behind a Presto Catalog

**Target:** 230–280 words, plus one architecture diagram

### Editorial necessity

Move from the mechanics of one query to the complete integration. This establishes CLP as an external Presto Native connector, not a sidecar or Java file reader.

### Presentation strategy

- Orient readers with familiar Hive/Iceberg connector concepts.
- State the break in the analogy: split-local structure remains authoritative for the long tail, so the catalog need not union every observed field.
- Show coordinator, metadata provider, native workers, shared library, and archives in one diagram.
- Explain the in-process boundary before discussing the upstream changes that enabled it.

### Required content

- Link Presto's concepts page and explain only the mappings needed here: the connector adapts CLP, the metadata provider discovers searchable KV-IR files and immutable archives, the convenience schema exposes common fields, and splits provide parallelism.
- State compactly that current OSS can query either format, including a mixed set of KV-IR and archive splits. Keep Uber's publication lifecycle, overlap precedence, and tier management out of this article.
- Qualify the detailed ERT, schema-tree, and object-storage walkthrough as the immutable archive path.
- Prefer this compact prose mapping over a second table; the architecture diagram already carries the structural explanation.

- Java coordinator: discovery, split planning, expression rewriting, and handle serialization.
- Native workers: connector registration, handle deserialization, CLP scan, and vector production.
- Each worker loads the connector plugin in-process; the plugin links CLP as a shared library. This avoids sidecar RPC, per-record language bridges, a Java rewrite, and raw-event materialization before search.
- Describe this as a native/shared-library boundary, not as permanently tied to C++.
- [PR #27653](https://github.com/prestodb/presto/pull/27653) enabled connector-factory registration from dynamically loaded native code; [PR #27652](https://github.com/prestodb/presto/pull/27652) routed connector-specific serialized values to native deserialization.
- Describe these PRs as completing important gaps, not creating all plugin infrastructure.
- Preserve attribution: YScope led connector implementation and named upstream work; IBM/the Presto community supported infrastructure and review; YScope and Uber extended and applied the architecture in production.

> CLP was not rewritten for Presto Native; Presto Native gained the extension path to load CLP's native engine as an independently developed connector.

### Audience benefit and story contribution

- **Audience benefit:** Presto users understand deployment boundaries, while connector authors understand coordinator/worker responsibilities and serialized contracts.
- **Story contribution:** Completes the end-to-end system picture so the next section can explain scale through avoided work.

---

## 5. Avoid Work at Every Layer

**Target:** 180–220 words

### Editorial necessity

Ground performance in specific mechanisms rather than an unsupported benchmark claim.

### Presentation strategy

For immutable archive splits, move from coarse to fine elimination:

1. **Metadata planning:** avoid archives using time bounds, indexed dimensions, statistics, and optional common-field views.
2. **Split-local resolution:** avoid schemas, ERTs, and columns using schema trees, types, logtypes, and dictionaries.
3. **Native encoded execution:** avoid raw-JSON reconstruction and decode only requested values or matching events.

Then show how the same philosophy shapes object-store reads and newest-first limited queries.

### Required content

- CLP places its header, schema tree, dictionaries, and indexes up front.
- The reader then advances sequentially with minimal seeking, without backward seeks or repeated jumps.
- State the ownership chain explicitly: a Presto Native worker is the compute process; it runs the CLP connector in-process; the connector opens an object-storage byte stream; CLP's streaming archive reader interprets and searches that stream as bytes arrive, without local archive caching or disk staging.
- Compression reduces transfer and scan work. Do not imply stream setup or range requests have zero cost.

**Uber production extension:** for eligible newest-first `ORDER BY timestamp DESC LIMIT N` queries, time-aware planning can stop creating older splits once metadata proves unseen archives cannot change the result.

- Conditions: append-only data, finite limit, trustworthy time bounds, ordered enumeration, and eligible filters.
- Impact: entire older archive ranges require no split generation, object-store read, or CLP scan.
- Presto schedules the selected splits normally; early termination changes which splits exist, not their execution order.
- Pruning proves an archive cannot match. Early termination proves it may match but cannot change the ordered limited result.

> Metadata determines what not to read; CLP makes what remains inexpensive to search.

### Audience benefit and story contribution

- **Audience benefit:** Operators can attribute performance to concrete elimination stages and understand the object-storage access pattern and early-termination benefit.
- **Story contribution:** Converts the architecture into a credible scale argument and exposes the reusable boundaries summarized next.

---

## 6. What Other Presto Native Connector Authors Can Reuse

**Target:** 80–120 words

### Editorial necessity

Generalize the integration. CLP is one of Presto Native's first independently developed, pluggable connector reference implementations, so its community value extends beyond log search.

### Presentation strategy

Present four lifecycle decisions, not a second deployment guide:

1. **Define the contract:** tables, types, splits, pushdowns, and nonstandard operations.
2. **Divide responsibilities:** coordinator planning/serialization versus worker registration/deserialization/scan.
3. **Package both halves:** coordinator artifact, shared library, dependencies, and compatible Presto/native-engine versions.
4. **Validate the boundary:** conventional and schema-less SQL, pushdown, missing fields, and serialization end to end.

### Required content

- Link the connector documentation for packages, configuration, and deployment instead of listing artifact formats in the article.
- Treat coordinator-to-native handles as a versioned protocol. Tests on each side do not replace a cross-boundary integration test.
- A domain team can own its engine and release lifecycle outside the Presto monorepo.
- Use **one of the first** until absolute chronology is verified.
- Avoid a large configuration block unless the publisher requests a quick start.

### Audience benefit and story contribution

- **Audience benefit:** Other Presto Native connector authors receive a concise lifecycle, packaging, ownership, and validation checklist.
- **Story contribution:** Generalizes CLP's implementation into community value before the final section establishes proof and future direction.

---

## 7. Open Source, Production Scale, and What Comes Next

**Target:** 140–180 words, including conclusion

### Editorial necessity

Close with evidence and boundaries. Readers need to know what is usable now, what Uber validates in production, and what remains future work.

### Presentation strategy

Use an evidence ladder: **current OSS → Uber production → future direction → conclusion**.

### Required content

**Current OSS:** end-to-end connector over searchable KV-IR files and immutable CLP archives, including mixed queries across both; dynamic field exposure; UDF rewriting; native filter/projection pushdown; typed exact-path and wildcard value search; complete-event JSON; pluggable metadata and split filtering.

**Uber production:** approved figures of approximately 25 PB/month raw, 750–850 TB/month compressed (roughly 29–33× depending on workload mix), served by five 48-core Presto Native workers directly from object storage. Label advanced planning, billion-file metadata scale, and early termination as Uber-only in the described deployment.

**Future direction:** one `event VARIANT` without changing CLP storage; schema-tree-backed key discovery and wildcard-field projection; possible summaries, structural-change detection, and candidate narrowing from compact metadata. Present these as directions, not commitments.

Conclude with three ideas only:

1. Schema-less does not have to mean row-blob execution.
2. Late-bound SQL exposure works because CLP columnizes observed structure during ingestion.
3. Query-plan resolution, pushdown, and native execution preserve that property inside Presto.

> CLP gives Presto one logical document, many native columns, and a query plan that knows how to reach them.

### Audience benefit and story contribution

- **Audience benefit:** Readers know what they can use now, what has been validated in production, and what remains directional.
- **Story contribution:** Resolves the credibility question and compresses the complete argument into the final “one logical document, many native columns” takeaway.

---

## Information Design

Use only three aids:

1. One SQL example in Section 3.
2. One four-row execution comparison table in Section 3.
3. One coordinator/native-worker architecture diagram in Section 4.

Avoid a second architecture diagram, a benchmark dashboard, a format-layout diagram, or a full API table. The article should explain one causal path, not document the entire system.

---

## Capability Labels

| Label | Meaning | Examples |
|---|---|---|
| **Current OSS** | Verifiable in the public CLP connector plugin or its documentation | KV-IR/archive and mixed-format queries, typed accessors, UDF rewriting, native pushdown, wildcard value search, complete-event JSON, pluggable metadata |
| **Uber production** | Deployed behavior not necessarily in OSS | Scale figures, object-store deployment, advanced planning, billion-file metadata, early termination |
| **Future direction** | Compatible with the architecture but not promised | `event VARIANT`, wildcard key projection, semantic candidate narrowing |
| **Comparative claim** | Requires workload/configuration context | Compression or query-performance ordering versus Parquet Variant |

---

## Publication Guardrails

- Use **CLP** consistently for the newer-generation engine discussed here.
- Use **native binary document column** for unshredded Parquet Variant; do not call it textual JSON or opaque.
- Do not claim Variant lacks pushdown or Iceberg cannot store schema-less data.
- “All fields are shredded” is an analogy; CLP is intrinsically columnized and needs no shredding operation.
- Distinguish logical column selection from exact physical byte isolation.
- Do not claim CLP universally outperforms Parquet Variant.
- Favor many recurring schemas. For near-singleton schemas, state that CLP coalesces small ERTs to control overhead, but this cannot restore absent structural repetition; the relative schema-grouping advantage over Parquet disappears.
- Keep current typed accessors/JSON output separate from future `event VARIANT`.
- Keep current wildcard value search separate from future key discovery/projection.
- State that current OSS supports KV-IR, immutable archives, and mixed queries; qualify ERT/schema-tree/archive-reader mechanics as the immutable archive path.
- Distinguish ordinary pruning, which proves no match, from early termination, which proves no effect on Top-N.
- Label current OSS, Uber production, and future directions at first mention.
- Preserve attribution among YScope, IBM/the Presto community, and Uber.

---

## Resolve Before Publication

- A same-corpus comparison specifying Parquet codec, Variant shredding policy, query set, and reader versions before publishing a direct multiplier or performance ordering.
- “One of the first” versus “the first” independently developed pluggable Presto Native connector.
- Exact build/package and Presto/Velox compatibility wording for the discussed release.
- Final verification of public wildcard semantics and path pushdown.
- Public-source or implementation-owner confirmation for the front-loaded header/schema-tree/dictionary/index layout and no-backward-seek reader behavior.
- Final contributor acknowledgments and approval for any semantic-analysis direction.

---

## Out of Scope

- Full Iceberg v3 or Trino Variant comparison.
- Full μSlope evaluation or CLP binary-format specification.
- Formal early-termination proof.
- Uber's complete ingestion, routing, and hot-tier architecture.
- Exhaustive UDF reference, installation tutorial, or build troubleshooting.
- Unverified performance multipliers, semantic-search costs, or product commitments.
