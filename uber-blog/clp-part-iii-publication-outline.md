# From 25 PB of Logs to Native SQL: CLP Part III Publication Outline

- **Status:** Publication strategy synchronized with the current draft and knowledge base
- **Audience:** Engineering readers familiar with SQL and large-scale data systems, but not necessarily with CLP or Presto Native internals
- **Article target:** 2,400–2,700 words; 3,000-word hard maximum
- **Primary goal:** Explain how Uber and YScope made continuously evolving logs queryable through Uber's SQL ecosystem while preserving CLP's compression and encoded execution
- **Secondary goal:** Show how work avoidance, object-storage-aware design, and a specialized in-process reader produce a small query-compute footprint at petabyte scale
- **Story boundary:** This is a production architecture case study, not a storage-format comparison or connector-development guide

---

## Intended Audience and Reading Level

### Primary audience

- Uber Engineering readers: infrastructure, observability, storage, data-platform, streaming, and machine-learning engineers.
- Engineering leaders evaluating how architectural choices affect cost, operability, and adoption at scale.
- Readers interested in the evolution of Uber's logging platform after Parts I and II.

### Secondary audience

- Operators and architects of large logging or event-data systems.
- Presto and CLP users interested in a production deployment rather than connector internals.
- Open-source readers evaluating whether the public CLP connector is relevant to their workloads.

### Assumed knowledge

- Familiarity with SQL queries, object storage, distributed workers, and hot/warm/cold data tiers.
- General awareness that query engines can push filtering and projection into storage readers.

### Not assumed

- CLP's archive format, schema trees, encoded record tables, logtypes, or variable encodings.
- Presto Native's coordinator/worker boundary, connector handles, or plugin loading.
- Uber's QueryRunner, Pinot integration, logging appenders, or internal metastore.
- Iceberg Variant, Parquet shredding, or the detailed comparison developed for the Presto community article.

The article must be understandable without following its links. Define unfamiliar terms briefly at first use; link research, documentation, or source code for readers who want implementation depth.

---

## Editorial Strategy

The article answers one production question:

> How can Uber make roughly 25 PB of continuously evolving logs per month searchable through familiar SQL without flattening the data, indexing every field, or building a correspondingly enormous query cluster?

The answer is causal rather than a list of platform features:

1. Uber's logs span natural-language messages, enriched events, and polymorphic JSON whose producers evolve independently.
2. Uber preserves one SQL entry point while routing between Pinot's indexed path and Presto Native with CLP's searchable KV-IR/archive path.
3. Fresh logs remain searchable in streaming-capable KV-IR files while asynchronous aggregation creates immutable CLP archives; one current-OSS query can span a mix of both formats, while metadata removes overlapping inputs so results are never duplicated.
4. CLP discovers and columnizes observed structure; the connector carries log-specific SQL intent through planning and pushdown into that storage model.
5. Metadata pruning and planning-time early termination avoid files and splits before execution.
6. The in-process connector dispatches mixed KV-IR/archive splits; CLP's streaming archive reader searches selected archive data directly from object storage.
7. Compression, work avoidance, and encoded execution together explain the five-worker production footprint.

The Presto article's deeper Variant and shredding analysis should improve the accuracy of this story without becoming a section in it. For Uber readers, the relevant point is positive and compact: CLP is columnar by design across recurring, evolving schemas, so teams do not have to predict every useful field before ingestion.

Assume the companion Presto article publishes first. Link it as **[PRESTO_BLOG_URL](PRESTO_BLOG_URL)** for readers who want the deeper Variant/shredding and connector discussion; replace the placeholder before publication.

### Narrative map

| Section | Reader question | Why needed | Audience benefit | Story contribution |
|---|---|---|---|---|
| Opening | How can five workers serve this scale? | Create a concrete production mystery | Readers see the outcome before learning the machinery | Establishes scale, cost discipline, and the two-part answer: avoid work, then search encoded data |
| 1. Data spectrum | What makes Uber's logs difficult? | Replace the false structured/unstructured binary with the actual workload | Platform engineers recognize schema ownership, polymorphism, and adoption constraints | Defines the data properties the query system must preserve |
| 2. SQL tension | Why not use fixed columns or one JSON string? | Explain why ordinary table modeling loses either flexibility or execution efficiency | Readers understand why the problem is not solved by syntax alone | Creates the need for storage-aware schema-less SQL |
| 3. Query architecture | Where does CLP sit in Uber's logging stack? | Place the solution inside the routed serving system and common query entry point | Readers understand path strengths, nonexclusive ownership, freshness, and object-storage flow | Connects the user experience to the physical serving architecture |
| 4. Native schema-less SQL | How does a SQL expression reach CLP's encoded columns? | Prove that the SQL surface preserves rather than hides CLP execution | Readers can follow one query from typed accessors through pushdown to the reader | Supplies the central technical mechanism |
| 5. Work avoidance | Why is the compute footprint so small? | Explain scale through specific elimination stages | Operators see what metadata, early termination, and streaming execution prevent | Resolves the opening mystery |
| 6. Production and OSS | What is proven, and what can readers use? | Separate deployed evidence from public functionality and future work | Readers can evaluate maturity without conflating Uber extensions with OSS | Converts architecture into credibility and adoption value |
| Conclusion | What should readers remember? | Compress the argument without introducing another roadmap | Readers leave with one durable systems lesson | Returns to the collaboration and production outcome |

### Presentation rules

- Lead with the production outcome, then reveal mechanisms in the order users encounter them.
- Introduce a concept only when it explains adoption, query behavior, or resource use.
- Prefer concrete examples—one polymorphic field, one SQL query, one Top-N plan—over exhaustive feature lists.
- Use **CLP** consistently for the newer-generation system discussed here. Use **CLP-S** only when quoting a historical name or source that requires it.
- Mark capabilities as **current OSS**, **Uber production**, or **future direction** at first mention.
- Keep collaboration visible without turning the article into contributor history.
- State scale honestly: 25 PB/month is produced by onboarded workloads, not scanned by every query.

### Section admission test

For every section, ask:

1. Which production question does this answer?
2. Why is this the right point in the story to introduce the detail?
3. What can the intended reader understand or decide afterward?
4. Which later claim does this section make credible?

Material that cannot answer all four belongs in documentation, the knowledge base, or a future article.

---

## Title Strategy

**Preferred:** **From 25 PB of Logs to Native SQL: Modernizing Logging at Uber with CLP (Part III)**

The title leads with the approved production scale, promises a user-visible outcome, and preserves continuity with Parts I and II. “Native SQL” must be earned in the body: supported SQL expressions are resolved and pushed into CLP rather than evaluated as record-at-a-time JSON wrappers.

**Alternative:** **How Uber Queries 25 PB of Evolving Logs with Presto and CLP**

Use the alternative if “Native SQL” is judged ambiguous. It is more literal but less connected to the article's central insight that SQL carries storage-native operations.

---

## Opening: The Scale–Simplicity Paradox

**Target:** 180–220 words

### Editorial necessity

Create the mystery that powers the article: a large and structurally difficult workload is served by a surprisingly small query tier. Readers should want an architectural explanation rather than interpreting the figures as a benchmark boast.

### Presentation strategy

- Open with the contrast among approximately 25 PB/month raw, 750–850 TB/month compressed, and five 48-core Presto Native workers.
- State that typical CPU use is 5–10% and that object-storage access is intentionally paced because responsive latency—not maximum throughput—is the objective.
- Establish direct object-storage search without local archive staging or caching.
- Name the two mechanisms that the article will unpack: metadata avoids work, and CLP searches what remains in encoded form.
- End by identifying the Uber–YScope collaboration and the article's question.

### Required content

- Scope the 25 PB figure to currently onboarded ML, streaming, and data-platform workloads.
- Distinguish monthly raw production from the amount any one query reads.
- Avoid repeating all detailed scale figures later in the opening; Section 6 will collect the proof.
- Do not introduce ERTs, UDF names, plugin infrastructure, or early-termination rules yet.

### Audience benefit and story contribution

- **Audience benefit:** Readers immediately understand why the system is operationally interesting.
- **Story contribution:** Establishes the outcome that every later mechanism must explain.

---

## 1. From Spark Logs to a Spectrum of Platform Data

**Target:** 280–330 words

### Editorial necessity

Define the workload before discussing query architecture. The difficult property is not JSON itself; it is continuous structural and type evolution across independently deployed producers.

### Presentation strategy

Move from series continuity to workload diversity:

1. Summarize Parts I and II in no more than three sentences.
2. Show expansion from Spark into representative ML/AI, streaming, and data-platform families.
3. Explain how appenders and native bindings enrich existing logs without application-wide rewrites.
4. Use one message-oriented and one JSON-oriented example to establish the structure continuum.
5. Define **semi-structured** and **polymorphic** only after readers have seen the examples.

### Required content

- Mention representative platforms—Michelangelo/Ray, Flink/Kafka, and Spark/Hudi—without implying all Uber services are onboarded.
- Explain that platform integrations add correlation, application, job, host, container, and related context while preserving natural-language messages.
- State that pattern configurations can become structured fields without rewriting individual logging calls.
- Define polymorphism with one path taking different types across producers or versions.
- Explain that historical and current records coexist, making evolution continuous rather than a periodic migration.
- Link the μSlope paper as the deeper research reference and attribute YScope, the University of Toronto, and the relevant Uber teams.

> At logging scale, schema evolution is not an occasional migration event. It is a continuous property of the data.

### Audience benefit and story contribution

- **Audience benefit:** Platform engineers see both the adoption constraint and the data-model problem.
- **Story contribution:** Defines what must survive the move into SQL: evolving paths, types, and natural-language context.

---

## 2. When SQL Meets Schema-Less Logs

**Target:** 260–320 words

### Editorial necessity

Explain why a conventional table schema or one JSON string cannot deliver both flexibility and storage-level execution. This creates the need for the connector without detouring into a general format comparison.

### Presentation strategy

- Start with Presto's ordinary planning contract: column names and result types must be known.
- Contrast that contract with producer-controlled paths, nesting, types, case, symbols, and SQL keyword collisions.
- Explain why central schema maintenance turns routine application changes into platform migrations.
- Treat a JSON string as a compatibility representation, not a sufficient physical execution model.
- Introduce the log-specific operations Uber needs, then explain why Presto remains strategically valuable.

### Required content

- Describe whole-document JSON cost precisely: repeated parsing and reconstruction, unused-field materialization, weaker field-level projection, and lost storage-native filtering.
- Do not call Parquet Variant an opaque blob or introduce Iceberg shredding; that comparison belongs in the Presto article.
- Frame key search, cross-field value search, and grep-like matching as required log-query semantics. Section 4 must label which interfaces are public, production-only, or future.
- Preserve Presto's role: QueryRunner integration, policy, routing, failover, aggregation, joins, and existing clients.

> The goal was not to make Presto accept JSON. It was to preserve Uber's SQL ecosystem while allowing CLP—not a generic JSON parser—to interpret and search the logs.

### Audience benefit and story contribution

- **Audience benefit:** Readers understand why the interface and storage engine must cooperate.
- **Story contribution:** Creates the transition from the data problem to Uber's tiered query architecture.

---

## 3. One Query Entry Point, Multiple Storage Tiers

**Target:** 220–280 words, plus one architecture figure

### Editorial necessity

Place CLP inside the system users actually experience. The article should show that SQL clients do not choose a storage engine and that CLP-backed access is not synonymous with offline or staged access.

### Presentation strategy

- Use one diagram to connect clients, QueryRunner, Pinot, Presto Native workers, the in-process CLP connector, and object storage.
- Explain the two routed paths' different strengths without presenting temperature ownership as exclusive.
- Follow the CLP branch far enough to establish the worker/connector/reader ownership chain.
- Close with freshness and publication-cadence tradeoffs.

### Figure 1: Uber log-query architecture

~~~text
Observability UI       Platform Query Builder       Programmatic Clients
         \                       |                         /
                           QueryRunner
                  policy · routing · failover
                              |
                +-------------+-------------+
                |                           |
              Pinot             Presto Native workers
       indexed random access   in-process CLP connector
                |                           |
          streaming data          metadata-coordinated
                                   mixed split planning
                                      /           \
                            searchable KV-IR   immutable CLP archives
                                      \           /
                                        object storage
~~~

### Required content

- QueryRunner preserves one SQL entry point and chooses the backend.
- Pinot provides indexed random access; Presto Native with CLP searches recent and older KV-IR/archive data from object storage.
- Routing is not an exclusive temperature boundary. Recently produced data is searchable through KV-IR, and selectively dual-ingested data can be available through both paths.
- Each Presto Native worker supplies compute and hosts the connector in-process. For archive splits, the connector opens the object-storage byte stream; CLP's streaming archive reader interprets and searches it as bytes arrive.
- There is no local archive cache or disk staging in the described Uber query path.
- Present KV-IR as an advancement of the streaming CLP file introduced in Part I, extending that low-buffering design to semi-structured logs.
- Logs first land in searchable, streaming-capable KV-IR files and are asynchronously aggregated into immutable CLP archives.
- **Current OSS:** The connector can plan and execute one query across a mixed set of KV-IR and archive splits.
- During overlapping coverage, Uber metadata and query planning prioritize the archived representation and exclude incorporated KV-IR inputs. Nonoverlapping files from both formats may contribute to the same result, but each event appears once.
- Current freshness through the KV-IR path is approximately 30 seconds to a few minutes.
- Explain the aggregation tradeoff among compression, freshness, and object-storage request cost.
- **Uber production:** Data that no longer justifies Pinot indexing can be offboarded to CLP object-storage access while remaining searchable. Selected data can later be “rehydrated” into Pinot when repeated low-latency access is again necessary.
- Note that CLP's low incremental storage cost makes selective dual ingest practical for workloads that need indexed random access and durable CLP access at the same time.
- Keep Pinot reuse of CLP ideas and implementation components to one clause; the indexing details remain out of scope.

### Audience benefit and story contribution

- **Audience benefit:** Readers understand the serving topology, tier boundaries, and freshness model.
- **Story contribution:** Provides the system context needed to trace a SQL expression into CLP next.

---

## 4. Bringing Native Schema-Less Data Access to Presto

**Target:** 480–560 words, including one compact SQL example

### Editorial necessity

This is the central technical section. It must prove that SQL reaches CLP's encoded structure rather than wrapping row-at-a-time JSON evaluation in function syntax.

### Presentation strategy

Move from storage model to user syntax to execution:

1. Explain CLP's intrinsic columnization in one compact paragraph.
2. Describe optional conventional columns and schema-less access as complementary interfaces.
3. Show one query using exact typed paths, wildcard value search, time/application filters, and Top-N.
4. Reveal that the functions are plan markers.
5. Trace the request through pushdown, split-local schema resolution, and encoded execution.
6. End with one concise contributor/infrastructure paragraph.

### Required content

#### Storage model

- A CLP archive contains an archive-level schema tree and many schema-homogeneous encoded tables.
- Records with the same keys and value types share a table; every schema node is columnized within that table.
- This avoids one global sparse union and does not require users to predict a promotion list before ingestion.
- One logical path may bind to several encoded columns across schema-specific tables within or across archives.
- CLP also separates recurring text patterns from typed variables inside string values; mention this briefly rather than re-explaining the compression algorithm from Parts I and II.
- Do not describe the schema tree as one dataset-global structure.

#### SQL surface

- An optional convenience schema can expose selected path-and-type bindings as ordinary columns after archives are written; it changes the logical interface, not CLP's physical layout.
- Paths outside that view remain reachable through schema-less accessors.
- Use verified public names in the example unless Uber production syntax is explicitly confirmed: `CLP_GET_STRING(path)`, `CLP_GET_BIGINT(path)`, `CLP_GET_JSON_STRING()`, and typed `CLP_WILDCARD_*_COLUMN()` value search.
- Typed accessors let Presto plan a stable result while preserving different types at the same logical path.
- Keep current typed wildcard **value search across fields** separate from wildcard key discovery or general key/value projection.
- If production-only key, regex, or message-search functions are discussed, label them as Uber production behavior and verify their exact semantics.
- Link the already-published companion Presto article at **[PRESTO_BLOG_URL](PRESTO_BLOG_URL)** rather than importing its full Variant/shredding comparison.

Use one query containing:

- A timestamp range and application identifier.
- Exact typed projection and a predicate on the same path.
- Typed wildcard value search across fields.
- `ORDER BY timestamp DESC LIMIT 100` to set up Section 5.

#### Execution path

Use this compact mechanism line:

> **SQL expression → connector rewrite and pushdown → split-local schema resolution → encoded CLP execution**

- The Java function bodies are planner markers, not record-at-a-time evaluators.
- Constant paths, requested types, and supported predicates are retained in the scan.
- At each split, CLP resolves paths against split-local structure; for archive splits, this maps them to existing encoded columns.
- Unsupported residual expressions remain in Presto.
- Selecting a complete event through JSON reconstruction requires more work than projecting one bound path.
- Avoid claiming perfect byte isolation; say CLP minimizes unrelated decompression and scanning.

#### Native integration and attribution

- The connector plugin runs inside each Presto Native worker and links CLP as a shared library; CLP's streaming reader performs the storage-specific search.
- In one short paragraph, credit YScope for leading the connector and named upstream work, IBM/the Presto community for infrastructure and review, and Uber/YScope for production integration.
- Keep dynamic loading and connector serialization to one sentence; detailed connector mechanics belong in the Presto article.

> SQL supplies the composition model; CLP supplies the schema-less semantics and encoded execution.

### Audience benefit and story contribution

- **Audience benefit:** Readers can connect user-facing SQL to physical column selection and understand why the UDF surface is not cosmetic.
- **Story contribution:** Establishes how CLP preserves its storage advantages inside Uber's SQL stack, enabling the resource argument in Section 5.

---

## 5. Avoid Work, Then Execute the Rest Efficiently

**Target:** 400–480 words, plus one work-elimination figure

### Editorial necessity

Resolve the five-worker mystery with concrete mechanisms. Compression alone does not explain the serving footprint; the system must also prove which files, schemas, columns, and encoded values need no work.

### Presentation strategy

Move from coarse to fine elimination:

1. Metadata pruning removes files that cannot match.
2. Planning-time early termination avoids older splits that cannot change a Top-N result.
3. Split-local schema resolution removes irrelevant tables and columns.
4. CLP searches the remaining encoded data and reconstructs only requested values or matching events.

Then connect these stages to object-storage layout and resource governance.

### Required content

#### Metadata as part of query execution

- The metastore tracks source URIs and formats, KV-IR/archive precedence, time bounds, record counts, selected top-level dimensions, available fields, and useful statistics.
- A pluggable implementation should support evolving indexed fields and incremental metadata results; do not turn this into a database-product tutorial.
- Uber's implementation handles billions of file records, but detailed schema and resource sizing remain out of scope.

#### Pruning versus early termination

- **Pruning** proves a file cannot match a predicate.
- **Early termination** proves an older candidate may match but cannot change an eligible newest-first limited result.
- State the important eligibility assumptions: append-only logs, finite `LIMIT`, trustworthy time bounds, time-aware enumeration, and compatible filters.
- Planning stops producing older splits only after correctness is proven; selected splits still use Presto's normal scheduler.
- If proof is unavailable, the system safely plans more data or falls back to the complete candidate set.

#### Object-storage and encoded execution

- CLP places its header, schema tree, dictionaries, and indexes up front, then supports forward sequential reading without backward seeks.
- For archive splits, the connector opens the object-storage stream and CLP begins interpreting and searching as bytes arrive without staging the complete archive.
- Compression reduces transferred bytes and scan work.
- API pacing is deliberate governance of shared infrastructure, not evidence that worker CPU is the bottleneck.

### Figure 2: Query-work elimination

~~~text
SQL predicates
      |
Metadata pruning
(remove files that cannot match)
      |
Planning-time early termination
(stop when older candidates cannot change Top-N)
      |
Split-local schema and column resolution
      |
Streaming encoded CLP search
(selective reconstruction; no local staging/cache)
      |
Small result returned to Presto
~~~

> The architecture scales by proving which data does not need to be read, then executing the remaining search inside the compressed format.

### Audience benefit and story contribution

- **Audience benefit:** Operators can attribute scale to specific planning and execution stages rather than an unexplained benchmark number.
- **Story contribution:** Supplies the causal explanation for the production evidence collected in Section 6.

---

## 6. Production Impact and Open-Source Boundary

**Target:** 220–280 words

### Editorial necessity

Close the evidence loop and distinguish what readers can use from what Uber added for its environment. This section should build trust, not repeat every number in the opening.

### Presentation strategy

Use an evidence ladder: **approved production figures → current OSS → Uber extensions → bounded future direction**.

### Required content

#### Approved Uber production evidence

- Approximately 25 PB/month of raw logs from onboarded workloads.
- Approximately 750–850 TB/month compressed: roughly 29–33× compression and about 97% less storage.
- Five 48-core Presto Native workers, typically at 5–10% CPU, serving recent and older CLP data directly from object storage.
- Approximately 30-second-to-few-minute freshness through searchable KV-IR files, followed by asynchronous aggregation into immutable CLP archives.
- Explain that responsive UX under deliberately paced object-storage access is the success criterion.
- **Uber production resilience:** The Kafka-fed hot/indexed path can experience rare loss during extreme overloads, while the CLP-backed object-storage path does not share this peak-loss mode.
- Explain why peak raw volume does not imply proportional CLP-backed object-storage traffic: bursts often contain loop-generated repetition, which raises CLP's compression ratio, and the object-storage path retains additional throughput headroom.

Do not include the current `[TBD]` performance table. Add at most one representative-query measurement only if every value is available, uses one consistent query definition, and distinguishes raw logical scope from compressed physical bytes read.

#### Current OSS

- Link the public CLP connector plugin and main CLP repository.
- Describe verified capabilities: end-to-end Presto access, native mixed KV-IR/archive queries, typed exact-path access, function rewriting, filter/projection pushdown, typed wildcard value search, complete-event JSON, and pluggable metadata/split filtering.

#### Uber production extensions

- Label advanced metadata planning, planning-time early termination, billion-file scale, routing, reliability, and operational controls as Uber deployment capabilities rather than current public behavior.
- Keep the hot-tier loss and peak-compression behavior scoped to Uber's described deployment rather than presenting it as a universal Kafka or object-storage property.
- State that Uber and YScope intend to contribute reusable components where practical without promising scope or timing.
- Include the double-digit-petabyte partner statement only if separately approved and accurately scoped.

#### Bounded future direction

- Use one clearly labeled sentence to explain that CLP archive metadata—including schemas, log-format patterns, and frequencies—could preload LLMs with compact structural context, narrow candidates for semantic search, and summarize recurring behavior without processing every repeated event independently.
- Do not claim this is deployed, GPU-free, or exceptionally inexpensive without a defined workload and measurement.

### Audience benefit and story contribution

- **Audience benefit:** Readers can distinguish demonstrated scale, usable open source, and deployment-specific engineering.
- **Story contribution:** Resolves the article's credibility question and prepares a concise conclusion.

---

## Conclusion: Schema-Less Does Not Have to Mean Opaque

**Target:** 130–170 words

### Editorial necessity

Compress the story into a durable systems lesson. Do not revisit semantic search in the conclusion or introduce edge execution or another detailed roadmap after the production proof.

### Presentation strategy

- Return to the Uber–YScope collaboration.
- Recap three ideas only: preserve evolving structure, carry SQL intent into CLP, and eliminate work before encoded execution.
- Close by resolving the title's scale contrast.
- Use at most one sentence to acknowledge future Pinot or broader CLP work without promising a venue.

### Required content

- Appenders enrich logs without application-wide rewrites.
- CLP preserves and columnizes evolving structure.
- Presto connects that model to Uber's existing query ecosystem.
- Metadata and CLP execution jointly explain the small serving tier.
- End on the production callback: 25 PB/month is accessible through one SQL experience served by five lightly utilized workers.

> Logs can retain their natural, evolving structure and still receive the projection, search, optimization, and composition expected from an analytical system.

### Audience benefit and story contribution

- **Audience benefit:** Readers leave with a transferable design principle rather than a product slogan.
- **Story contribution:** Resolves the opening mystery: petabyte-scale logs do not inherently require a petabyte-scale query cluster.

---

## Information Design

Use only three aids:

1. One tiered query-architecture figure in Section 3.
2. One compact SQL example in Section 4.
3. One work-elimination figure in Section 5.

Remove the incomplete representative-performance table unless a complete, consistently scoped measurement is available. Avoid a storage-format diagram, connector-lifecycle checklist, full API table, metastore schema, or second SQL example.

---

## Capability Labels

| Label | Meaning | Examples |
|---|---|---|
| **Current OSS** | Verifiable in the public connector or CLP documentation | Mixed KV-IR/archive queries, typed accessors, rewriting and pushdown, typed wildcard value search, complete-event JSON, pluggable metadata |
| **Uber production** | Deployed behavior not necessarily available publicly | Scale figures, overlap precedence, tier offboarding/rehydration, peak resilience, advanced metadata, early termination, routing and controls |
| **Future direction** | Architecturally compatible but not promised | Broader key discovery, Pinot details, LLM context preloading, semantic search, and summarization |
| **Research evidence** | Supported by the linked μSlope paper | CLP's compression and encoded-search design |
| **Comparative or performance claim** | Requires a controlled definition and measurement | Query latency, pruning percentage, throughput, or comparison with another format |

---

## Publication Guardrails

- Use **CLP** consistently for the newer-generation engine; avoid alternating with CLP-S.
- Scope 25 PB/month to onboarded workloads and never imply that each query scans that volume.
- Keep raw volume, compressed footprint, and physical bytes read distinct.
- Describe the authoritative schema tree as archive/split-local, not one dataset-global contract.
- Explain that schema grouping is columnar by design without importing the full Iceberg/Parquet shredding comparison.
- Use verified function names and distinguish current public value search from key discovery or general wildcard projection.
- Do not describe planner-marker functions as record-at-a-time UDF execution.
- Preserve the ownership chain: worker supplies compute; connector runs in-process; CLP streaming reader interprets and searches the byte stream.
- Distinguish searchable KV-IR files from immutable CLP archives. State that current OSS can query a mix of both formats, and explain that Uber's overlap handling removes only incorporated KV-IR inputs so each event appears once.
- Describe offboarding, selective rehydration, and dual ingest as Uber tier-management choices, not automatic current-OSS policies.
- Do not present Pinot as exclusively hot or CLP as exclusively warm/cold; recent data is searchable through KV-IR, and selective dual ingest can make data available through both paths.
- Keep rare hot/indexed-path loss, repetition-driven burst compression, and object-storage throughput headroom scoped to Uber's described deployment.
- Distinguish logical column selection from exact physical byte isolation.
- Distinguish metadata pruning from planning-time early termination.
- Separate current OSS, Uber production behavior, and future directions at first mention.
- Do not imply five workers are undersized or that higher throughput is the objective; resource restraint is intentional.
- Preserve attribution among Uber, YScope, IBM/the Presto community, and other contributors without interrupting the system narrative.

---

## Resolve Before Publication

- Replace every `PRESTO_BLOG_URL` placeholder with the published companion Presto article URL and verify the link.
- Reverify the public function names, dot-path syntax, and one-sided wildcard pattern used in the SQL example against the publication release.
- Which key, regex, message, and cross-field operations are current Uber production behavior versus RFC or future design.
- Whether the 5–10% CPU range and 30-second-to-few-minute freshness remain current for the stated deployment scope.
- Final approval for the rare hot/indexed-path loss, CLP-backed peak-resilience, loop-burst compression, and object-storage throughput-headroom wording.
- Final approval for offboarding, selective rehydration, and dual-ingest wording.
- Whether one complete representative-query measurement is available; otherwise remove the performance table entirely.
- Final wording and approval for the double-digit-petabyte partner statement.
- Public-source or implementation-owner confirmation of the front-loaded archive metadata and no-backward-seek reader behavior.
- Final contributor acknowledgments and organization naming.

---

## Out of Scope

- Detailed Iceberg Variant or Parquet shredding comparison.
- Full CLP archive-format or compression-algorithm derivation.
- Presto Native connector lifecycle, packaging, or serialization guide.
- QueryRunner internals beyond routing, policy, and failover.
- Full Pinot schema discovery, Lucene tokenization, or hot-tier search design.
- Complete Uber ingestion, metastore, routing, or observability architecture.
- Formal early-termination proof or pseudocode.
- Exhaustive UDF documentation.
- Incomplete benchmark tables or unsupported comparative multipliers.
- Detailed semantic-search, local/edge execution, or publication-roadmap promises.
- Claims that thousands of Uber services are already onboarded.
