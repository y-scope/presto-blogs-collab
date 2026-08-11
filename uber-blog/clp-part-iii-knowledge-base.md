# CLP Part III Knowledge Base

- **Status:** Synchronized source of truth for the Uber Engineering draft and publication outline
- **Updated:** August 10, 2026
- **Working title:** *From 25 PB of Logs to Native SQL: Modernizing Logging at Uber with CLP (Part III)*
- **Publication target:** Uber Engineering Blog
- **Purpose:** Preserve approved facts, technical mechanisms, capability boundaries, attribution, editorial constraints, and remaining publication work behind the concise article

This document is intentionally more detailed than the article. The draft should remain production-led and accessible to Uber Engineering readers. Deeper storage-format, Variant/shredding, and connector-development details belong in the companion Presto article, assumed to publish first at **[PRESTO_BLOG_URL](PRESTO_BLOG_URL)**.

---

## 1. Executive Summary

- Currently onboarded machine-learning/AI, streaming, and data-platform workloads generate approximately **25 PB of raw logs per month**.
- CLP reduces that monthly volume to approximately **750–850 TB**:
  - Roughly **29–33× compression**
  - Approximately **97% less stored volume**
- Five 48-core Presto Native workers serve recent and older data through searchable KV-IR files and immutable CLP archives directly from Uber's internal object storage.
- Worker CPU utilization is typically approximately **5–10%**.
- The objective is responsive user latency with minimal use of shared infrastructure, not maximum throughput.
- Uber intentionally paces object-storage API use; low CPU does not imply a need to increase scan throughput.
- No individual query scans the complete monthly ingest volume.
- The system supports a continuum from natural-language messages to enriched and native JSON events whose schemas and types evolve continuously.
- Fresh logs first land in searchable, streaming-compressed KV-IR files and are asynchronously aggregated into immutable CLP archives.
- A log collector runs on every host, monitors container logs, enriches each event, and incrementally compresses it into an appendable KV-IR file.
- The active KV-IR file is already compressed as it grows; archive aggregation is not where compression begins.
- On a configurable timeout, the collector uploads the current file extent without closing the stream; later events extend the same KV-IR file.
- Each file update produces a metadata event on Kafka, which feeds the metastore used for query discovery.
- A single query can span a mixed set of KV-IR and archive splits.
- Where KV-IR and archive coverage overlaps, Uber prioritizes the archived copy and excludes the incorporated KV-IR input, so both formats can contribute to a query without returning duplicate events.
- CLP archives contain archive-local schema trees and schema-homogeneous encoded record tables; there is no authoritative dataset-global schema tree.
- An optional convenience schema is a late-bound logical view over columns that already exist in CLP storage.
- Public typed accessors and wildcard value-search functions are planner markers. The connector rewrites supported expressions and pushes their intent into split-local CLP execution.
- The execution chain is:

> **SQL expression → connector rewrite and pushdown → split-local schema resolution → encoded CLP execution**

- Scale comes from two complementary effects:
  - Metadata avoids files and splits before execution.
  - CLP minimizes scanning, decompression, and reconstruction for the selected encoded data.
- Uber additionally uses planning-time early termination for eligible newest-first limited queries. Selected splits still use Presto's normal scheduler.
- Current OSS already supports end-to-end Presto querying, including mixed KV-IR/archive queries, typed exact-path access, filter/projection pushdown, typed wildcard value search, complete-event JSON, and pluggable metadata/split filtering.
- Uber production adds advanced metadata planning, billion-file scale, early termination, routing, reliability, and operational controls.

### Central thesis

> Logs can retain their evolving structure and still receive the projection, search, optimization, and composition expected from an analytical system.

### Production resolution

> By preserving evolving structure, eliminating work early, and searching encoded data in place, Uber made 25 PB per month of logs accessible through one SQL experience—served by five lightly utilized workers.

---

## 2. Publication Narrative and Boundaries

The article answers one end-to-end question:

> How can Uber accept continuously evolving logs from independent producers, preserve them economically, and make approximately 25 PB per month searchable through familiar SQL without flattening the data, indexing every field, or building a correspondingly enormous query cluster?

The causal answer is:

1. Uber's independently deployed producers emit natural-language messages, enriched events, and polymorphic JSON that cannot share one complete global schema.
2. Per-host collectors preserve this evolving structure, enrich events, and incrementally compress them into appendable KV-IR; periodic data uploads and Kafka metadata updates make growing compressed files discoverable without closing them.
3. Uber preserves one SQL entry point while a metastore selects searchable KV-IR/archive inputs for Presto Native with CLP.
4. Current OSS can query a mix of both CLP formats; Uber removes overlapping KV-IR inputs once their archived copy is authoritative.
5. CLP columnizes observed structure within archive-local, schema-homogeneous tables.
6. Connector rewriting carries SQL intent into split-local storage resolution and encoded execution instead of forcing row-oriented JSON materialization.
7. Metadata pruning and planning-time early termination avoid work before workers read object storage.
8. Ingestion/storage separation, compression, work avoidance, and encoded execution together explain the production architecture and five-worker query footprint.

### Intended audience

- Uber infrastructure, observability, storage, data-platform, streaming, and machine-learning engineers
- Engineering leaders evaluating cost, reliability, operability, and adoption at scale
- Presto and CLP users interested in production architecture rather than connector internals

### Article shape

- Opening: 170–210 words
- Evolving logs and schema coordination: 300–360 words
- Ingestion lifecycle: 240–300 words, plus one figure
- SQL entry point and storage paths: 230–280 words
- Native schema-less access: 400–470 words, including one SQL example
- Work avoidance: 340–420 words, plus one figure
- Production/OSS/collaboration boundary: 280–340 words
- Conclusion: 130–170 words
- Total target: 2,200–2,600 words; 3,000-word hard maximum

### Story boundary

- Production architecture case study, not a complete connector guide
- Explain the per-host collector-to-KV-IR-to-archive lifecycle, but treat the metastore implementation as a black box rather than turning the article into a complete ingestion-service reference
- No full Iceberg Variant or Parquet shredding comparison
- No complete CLP archive-format derivation
- No formal early-termination proof
- No large benchmark dashboard
- No exhaustive API reference
- No detailed Pinot/Lucene implementation
- No edge-execution roadmap in the conclusion

---

## 3. Series Continuity and Workload

### Part I

- [Reducing Logging Cost by Two Orders of Magnitude Using CLP](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/)
- Introduced streaming CLP compression for Spark.
- Reduced logging cost, mitigated SSD write amplification, and enabled longer retention.

### Part II

- [Modernizing Logging at Uber with CLP (Part II)](https://www.uber.com/en-GB/blog/modernizing-logging-with-clp-ii/)
- Expanded the work into end-to-end ingestion, retention, viewing, and programmatic analysis.

### Part III

- Expands from Spark into representative platform families:
  - ML/AI: Michelangelo and Ray
  - Streaming: Flink and Kafka
  - Data platforms: Spark and Hudi
- Do not imply that every Uber service or thousands of services are already onboarded.
- Scope the 25 PB/month figure to currently onboarded workloads.

### Appenders and native bindings

Uber and YScope jointly developed language-specific appenders and bindings that:

- Preserve existing natural-language messages.
- Translate existing pattern configurations into structured fields.
- Add correlation, platform, application, job, host, container, and related context.
- Avoid broad rewrites of application logging statements.

The adoption lesson is important: structure is added at the platform boundary rather than through application-wide migrations.

### The data spectrum

- **Message-oriented logs:** meaning primarily lives in natural language with embedded variables.
- **Enriched application logs:** retain the message while adding structured operational context.
- **Native JSON events:** meaning primarily lives in request, state, input, and outcome fields, but may still contain natural-language descriptions.

JSON is an encoding syntax, not a stable data contract.

### Definitions

- **Semi-structured:** fields and nesting are discoverable, but one centrally enforced schema does not describe every event.
- **Polymorphic:** one logical path may contain different types across events, producers, versions, or time.

Example:

```json
{"status": 500}
{"status": "FAILED"}
{"status": {"code": 500}}
```

This is not necessarily a data-quality failure. Independent services evolve, languages differ, and historical and current events coexist.

Research reference: [μSlope: High Compression and Fast Search on Semi-Structured Logs, OSDI '24](https://www.usenix.org/conference/osdi24/presentation/wang-rui). Attribute the work to YScope, the University of Toronto, and Uber's Observability, Pinot, and Storage and Search teams.

---

## 4. Ingestion and Storage Lifecycle

### Per-host collection and publication

- A log collector runs on every host and monitors logs from containers on that host.
- The collector enriches events before incrementally compressing them into KV-IR.
- KV-IR is compressed incrementally and appendable: the collector keeps the active compressed file open as new records arrive.
- On a configurable timeout, the collector uploads the current file extent without closing the active stream.
- Subsequent updates publish the newly appended data.
- Every file update also emits a metadata update to a Kafka topic.
- The metastore consumes these updates so the query path can discover the growing KV-IR files.
- The article treats the metastore as a black box; Pinot implementation details are irrelevant to this ingestion explanation.

### KV-IR

- [KV-IR](https://docs.yscope.com/clp/main/dev-docs/design-kv-ir-streams/index.html) is the streaming-compressed key-value internal-representation format for dynamically structured logs.
- Present KV-IR as an advancement of the streaming CLP file introduced in Part I, extended to semi-structured logs.
- KV-IR buffers less data than the archive format and is suitable for low-latency and resource-constrained ingestion.
- Fresh KV-IR files are searchable; users do not wait for archive creation.

### Archive aggregation

- Asynchronous jobs transform already compressed KV-IR files into denser immutable CLP archives.
- Aggregation improves long-term compression density and layout; it does not mark the beginning of compression.
- Archives provide the longer-term compression and search representation.
- Current freshness through the KV-IR path is approximately 30 seconds to a few minutes.

### Mixed-source queries and overlap handling

- Current OSS supports a single query over a heterogeneous split set containing KV-IR files and CLP archives.
- One time range may require files from both formats.
- Aggregation creates temporary coverage overlap.
- Uber metadata records the relationship between incorporated KV-IR inputs and their archived representation.
- For overlapping coverage, the archived copy is authoritative and the incorporated KV-IR input is excluded.
- Nonoverlapping KV-IR and archive files may both contribute to the same query.
- Each event appears once; the system does not choose one format for the entire query.

### Publication tradeoffs

- More frequent publication improves freshness but increases object-store requests.
- Larger aggregation units generally improve compression and reduce request overhead.
- The chosen cadence balances freshness, compression, and shared object-storage use.

---

## 5. Query Architecture and Tier Management

The current Part III article treats the metadata implementation as a black-box **metastore** and does not explain Pinot or broader tier-management internals. The details below are retained only as background and capability guardrails.

### User-facing architecture

- Observability UI
- Platform query builders
- Programmatic clients
- Uber's SQL query and routing layer, including QueryRunner and related infrastructure

Use QueryRunner attribution carefully. It participates in parsing, governance, routing, cluster selection, and failover, but responsibilities may span related services.

### Routed serving paths

- **Metastore:** discovers relevant KV-IR and archive inputs from metadata updates; its internal implementation is outside the article.
- **Presto Native with CLP:** searches the selected recent KV-IR and immutable-archive data in Uber's internal object storage.
- Users retain one SQL-oriented experience rather than choosing a storage engine directly.

### Worker, connector, and reader ownership

- A Presto Native worker supplies compute.
- The CLP connector runs in-process inside the worker.
- The connector plans/dispatches the appropriate reader for KV-IR or archive splits.
- For archive splits, the connector opens the object-storage byte stream and CLP's streaming archive reader interprets and searches it as bytes arrive.
- No local archive staging or cache exists in the described Uber query path.

### Flexible tier lifecycle

These are Uber production tier-management choices, not automatic current-OSS policies:

- Data that no longer justifies Pinot indexing can be offboarded to CLP object-storage access while remaining searchable.
- Selected data can later be **rehydrated** into Pinot if repeated low-latency access again justifies hot indexing.
- Selected streams can be dual-ingested when indexed random access and durable CLP access are both required.
- CLP's low incremental storage cost makes selective dual ingest practical.

### Peak-volume resilience

- The hot/indexed path is fed through Kafka and can experience rare log loss during extreme overloads.
- The CLP-backed object-storage path does not share this peak-loss mode.
- Extreme log bursts often come from loop-generated repetition.
- Increased repetition raises CLP's compression ratio.
- Therefore, a large spike in logical/raw volume need not create a proportional rise in compressed object-storage traffic.
- The object-storage path retains additional throughput headroom.

Keep these claims scoped to Uber's described deployment. Do not present them as universal Kafka or object-storage properties, and do not claim unlimited capacity.

### Pinot scope

- Pinot also reuses CLP ideas and implementation components.
- Details of schema discovery, tokenization, Lucene indexing, and random-access search remain out of scope.

---

## 6. Why SQL Needs Storage-Aware Schema-Less Access

### Presto's normal planning contract

Presto expects relation shape, column names, and expression types during planning. Evolving logs violate that expectation:

- Fields appear and disappear.
- Nesting changes.
- One path may hold multiple types.
- Keys may contain case distinctions, symbols, spaces, or SQL keywords.

### Why a manually maintained schema fails

- No central team can predict every producer's fields.
- Routine application changes become table migrations.
- A canonical type can lose meaning or fail on a later variant.
- Historical and current structures coexist.

### Why one JSON string is insufficient

- Repeated complete-document parsing
- Runtime type discovery
- Materialization of unused fields
- Weaker field-level projection and filtering
- Loss of CLP's encoded execution advantages

The article should not claim that every binary dynamic-document format has these costs. The detailed Variant/shredding comparison belongs in the companion Presto article.

### Why retain SQL

- Existing clients and interfaces
- Governance, routing, and failover
- Aggregation, joins, ordering, and limits
- Familiar analytical composition

The goal is not merely to make Presto accept JSON. It is to carry log-query intent through SQL planning into CLP execution.

---

## 7. CLP Archive Storage Model

### Structural scope

- A CLP archive is an independently searchable storage unit and typically a Presto split.
- Each archive contains an archive-level schema tree describing the paths and types present in that archive.
- The authoritative structure is archive/split-local, not dataset-global.
- Different archives may contain different paths, nesting, and type variants.

### Schema-homogeneous encoded tables

- Records with the same keys and value types share an encoded record table (ERT).
- Each schema node becomes an encoded column within that ERT.
- One archive contains many ERTs behind one archive-local schema tree.
- This avoids one dataset-wide sparse union and does not require a promotion list chosen before ingestion.
- One logical path may map to multiple encoded columns across tables within one archive or across archives, including typed variants.

### Intrinsic columnization

- Every observed schema node is columnized within its schema-homogeneous table.
- This is intrinsic to ingestion, not a later shredding operation.
- Safe claim: CLP minimizes unrelated decompression and scanning.
- Unsafe claim: CLP never fetches or decompresses any unrelated byte.
- Physical compression-stream packaging can affect byte-level reads even when logical column selection is exact.

### Logtype and variable encoding

CLP also finds recurring structure inside string values:

```text
Request 123 failed after 45 ms
Request 987 failed after 12 ms
                ↓
Request <int> failed after <int> ms
```

The placeholders are explanatory rather than literal on-disk syntax. CLP stores recurring text and typed variables separately using appropriate encodings.

### Convenience schema

- Optional selected path-and-type bindings appear as ordinary SQL columns.
- The convenience schema can be changed after archives are written.
- Changing it does not rewrite or re-encode CLP data.
- It is a late-bound logical view over ERT columns that already exist.
- Paths outside the view remain accessible through schema-less functions.

Preferred formulation:

> The convenience schema is late-bound projection over already-columnized storage.

---

## 8. Current SQL Surface and Capability Boundaries

### Verified current OSS exact-path accessors

- `CLP_GET_BIGINT(path)`
- `CLP_GET_DOUBLE(path)`
- `CLP_GET_STRING(path)`
- `CLP_GET_BOOL(path)`
- `CLP_GET_STRING_ARRAY(path)`
- `CLP_GET_JSON_STRING()` for complete-event reconstruction

Examples in public tests use constant dot-separated paths such as `user_id` and `city.Name`. Do not publish `$`-prefixed JSONPath syntax unless verified for the release being discussed.

### Verified current OSS wildcard value search

- `CLP_WILDCARD_STRING_COLUMN()`
- `CLP_WILDCARD_INT_COLUMN()`
- `CLP_WILDCARD_FLOAT_COLUMN()`
- `CLP_WILDCARD_BOOL_COLUMN()`

These expressions search typed **values across fields whose names the query does not enumerate**.

Keep separate:

- **Current OSS:** typed wildcard value search across fields
- **Future:** key discovery, wildcard key matching, and general `{path, value}` projection
- **Uber production:** additional log-search operations whose public names and exact semantics must be labeled separately if mentioned

Do not use or imply unverified public functions such as `CLP_GET_INT`, `CLP_GET_FLOAT`, or untyped `CLP_WILDCARD_COLUMN()`.

### Example

```sql
SELECT
    timestamp,
    application_id,
    CLP_GET_STRING('event.request_id') AS request_id,
    CLP_GET_BIGINT('event.status_code') AS status_code
FROM clp.platform_logs
WHERE timestamp >= current_timestamp - INTERVAL '30' DAY
  AND application_id = 'example-application'
  AND CLP_GET_BIGINT('event.status_code') >= 500
  AND CLP_WILDCARD_STRING_COLUMN() LIKE 'OutOfMemory%'
ORDER BY timestamp DESC
LIMIT 100;
```

Use only release-verified wildcard patterns. Public tests cover equality, numeric predicates, and one-sided `LIKE` patterns; do not assume two-sided `%value%` pushdown without verification.

### Whole-event access

- `CLP_GET_JSON_STRING()` reconstructs the complete event.
- Complete-event selection requires more work than projecting one bound path.
- It does not produce one dynamically shaped Presto `ROW` containing every observed field.

---

## 9. Rewriting, Pushdown, and Encoded Execution

### Planner-marker functions

- CLP functions look like UDFs at the SQL surface.
- Their Java bodies are plan markers, not row-at-a-time JSON evaluators.
- The connector recognizes supported constant paths, requested types, projections, and predicates.
- It carries that intent in connector scan handles.
- Unsupported residual expressions remain in Presto.
- Partial pushdown must remain correctness preserving; it cannot drop unsafe branches of an `OR` predicate.

### Execution chain

> **SQL expression → connector rewrite and pushdown → split-local schema resolution → encoded CLP execution**

1. Presto parses and type-checks the expression.
2. Connector rewriting retains constant paths, requested types, and supported operations.
3. Pushdown moves compatible filtering and projection into the CLP scan.
4. At each split, CLP resolves paths against split-local structure.
5. For an archive split, paths bind to the relevant ERTs and encoded columns.
6. CLP searches/projects encoded data and reconstructs only requested values or matching events.
7. Presto evaluates unsupported residual work and performs downstream relational operators.

### Ownership chain

- Presto Native worker: compute process
- In-process connector: Presto-to-CLP integration and split dispatch
- CLP reader: storage-specific interpretation, search, and projection

Avoid phrasing that makes Presto itself the storage layer or makes the connector a separate remote service.

Memorable formulation:

> SQL supplies the composition model; CLP supplies the schema-less semantics and encoded execution.

---

## 10. Object-Storage Access

### Archive layout

- Header, archive-local schema tree, dictionaries, and indexes appear up front.
- After initialization, the reader advances forward sequentially without backward seeks.
- The connector can begin archive search as bytes arrive rather than staging the complete object.
- Compression reduces transfer and scan work.
- Object-store stream setup, requests, and encoded-region fetches still have costs.

### Uber production path

- KV-IR and archive files reside in internal object storage.
- No local data-disk staging.
- No local archive/data cache in the described query path.
- Object-storage API pacing is deliberate governance of shared infrastructure.
- The target is sufficient latency with minimal shared-resource consumption.
- Additional CPU or unpaced throughput would not materially improve the intended user experience.

### Claim boundary

Safe:

- Search begins as bytes arrive.
- Reader uses forward sequential access without backward seeks.
- CLP minimizes unrelated decompression and scanning.

Avoid:

- Zero request or stream-setup cost
- Perfect byte isolation
- Unlimited object-store capacity
- Claims that worker CPU is the current bottleneck

---

## 11. Metadata, Pruning, and Early Termination

### Metastore role

The metastore is part of query planning. It may track:

- Source URI and format
- KV-IR-to-archive incorporation/precedence state
- Minimum and maximum timestamp
- Record count
- Selected top-level dimensions
- Available paths/types
- Useful file-level statistics
- Lifecycle and retention state

Uber's relational implementation manages metadata for billions of file records. Detailed schema and resource sizing are not required in the article.

The connector is metastore-agnostic. A provider should support evolving indexed fields, efficient lookup, incremental/streaming results, and a defined consistency model.

### Metadata pruning

Question:

> Could this file contain any matching event?

- Predicate-driven
- Removes files that cannot match
- Independent of ordering and limits
- Applies before split scheduling and object-store reads
- Can operate over a mixed KV-IR/archive candidate set

### Planning-time early termination

Question:

> Could this older candidate still change the ordered limited result?

Eligible shape:

```sql
ORDER BY timestamp DESC
LIMIT N
```

Important prerequisites:

- Append-only logs
- Finite limit
- Recognized timestamp ordering
- Trustworthy file time bounds
- Time-aware enumeration
- Compatible filters and correctness-preserving counts/bounds

High-level behavior:

1. Apply ordinary pruning.
2. Process surviving candidate-file metadata in timestamp order.
3. Track counts/bounds and a result watermark.
4. Include overlapping time ranges that may affect global Top-N.
5. Stop generating older splits only when correctness is proven.
6. If proof is unavailable, select more files or the complete pruned set.
7. Let Presto schedule all selected splits normally.

Early termination changes which splits exist, not their execution order. It is an Uber production extension, not current public connector behavior.

### Distinction

- **Pruning:** file cannot match the predicate.
- **Early termination:** file may match, but cannot change the newest-first limited result.

Preferred formulation:

> The system first proves which files need not become splits, then CLP makes the selected encoded data inexpensive to search.

---

## 12. Current OSS, Uber Production, and Future Directions

### Current OSS

- End-to-end Presto access
- Mixed KV-IR/archive queries
- Typed exact-path access
- Function rewriting
- Filter and projection pushdown
- Typed wildcard value search across fields
- Complete-event JSON
- Pluggable metadata, split, and split-filter providers
- Read-only, append-oriented workflow

Do not describe the open-source connector as a preview or merely an architectural reference. It is usable end to end.

### Uber production

- Approximately 25 PB/month raw onboarded logs
- Approximately 750–850 TB/month compressed
- Five 48-core workers at approximately 5–10% CPU
- Approximately 30-second-to-few-minute freshness through KV-IR
- Direct object-storage access without local staging/cache
- Metadata for billions of file records
- Advanced metadata filtering and planning
- Mixed-format precedence/de-duplication policy
- Planning-time early termination
- Query routing, reliability, and operational controls
- Pinot offboarding and selective rehydration
- Selective dual ingest
- Peak-volume resilience behavior described in Section 5

Uber and YScope intend to contribute reusable components where practical, without promising scope or timing.

### Future directions

- Schema-tree-backed key discovery
- General wildcard key/value projection
- More natural Variant-based SQL ergonomics if PrestoDB gains end-to-end Variant support
- Summarization and structural-change detection
- Compact LLM context preloading from archive metadata
- Semantic-search candidate narrowing

Future directions are architectural possibilities, not product commitments.

---

## 13. Production Evidence and Metric Discipline

### Approved figures

- Raw volume: approximately 25 PB/month for onboarded workloads
- Compressed volume: approximately 750–850 TB/month
- Compression: approximately 29–33×
- Stored-volume reduction: approximately 97%
- Query workers: five
- Worker cores: 48 each
- Typical CPU utilization: approximately 5–10%
- Freshness: approximately 30 seconds to a few minutes
- Storage path: internal object storage
- No local archive cache or disk staging

### Resource philosophy

- Do not present low CPU as an undersized cluster or optimization failure.
- Object-storage requests are deliberately paced.
- Responsive UX is the success criterion.
- Maximum throughput is not the goal.

### Burst behavior

- Rare hot/indexed-path losses occur only under extreme overload conditions in the described deployment.
- CLP-backed object-storage retention does not share that failure mode.
- Loop-generated bursts contain unusually high repetition.
- Compression ratio can rise during those bursts.
- Distinguish logical raw volume from compressed object-storage traffic.
- Do not claim every spike compresses well; scope the mechanism to the common loop/repetition pattern.

### Benchmark discipline

The incomplete performance table has been removed. Add at most one representative query only if every value is available from the same query definition:

- Candidate files before pruning
- Files after pruning
- Splits after early termination
- Compressed physical bytes read
- Object-storage requests
- End-to-end latency

Always distinguish:

- Monthly raw ingest volume
- Logical raw scope represented by a query
- Compressed physical bytes read
- Files/splits scheduled

---

## 14. Collaboration and Attribution

### Uber and YScope

- Long-running engineering collaboration
- Joint appender and platform integration work
- Joint production architecture, validation, and operations
- YScope led the open-source connector implementation
- Uber supplied production requirements, data diversity, infrastructure integration, scale, and operational validation

### Presto Native infrastructure

- The connector helped complete important gaps in the out-of-band Presto Native connector path.
- YScope contributors ChenXing Yang and Jack Luo led key upstream changes for dynamic plugin loading and connector serialization.
- IBM engineers and the broader Presto community contributed infrastructure, review, and design feedback.
- Do not claim YScope created all plugin infrastructure.
- Use **one of the first independently developed pluggable Presto Native connectors** until absolute chronology is verified.

Relevant upstream changes:

- [PR #27653](https://github.com/prestodb/presto/pull/27653): connector-factory registration from dynamically loaded native code
- [PR #27652](https://github.com/prestodb/presto/pull/27652): connector-specific serialized values routed to native deserialization

Keep detailed packaging, ABI, handle serialization, and factory-map mechanics in the companion Presto article.

---

## 15. Semantic Search and Summarization

### Bounded public direction

Safe article wording:

> **Future direction:** CLP archive metadata—including schemas, log-format patterns, and frequencies—could preload LLMs with compact structural context, narrow candidates for semantic search, and summarize recurring behavior without processing every repeated event independently.

The mechanism is plausible because CLP retains:

- Archive-local schema structure
- Logtype and log-format-pattern dictionaries
- Typed variables
- Pattern frequencies
- Time bounds and statistics

Potential future flow:

1. Summarize archive schemas, log-format patterns, frequencies, and time/statistical context.
2. Preload that compact representation into an LLM instead of beginning with repeated raw events.
3. Narrow candidate schemas, logtypes, or encoded regions using archive metadata.
4. Apply semantic analysis or summarization to the reduced candidate set.
5. Map relevant patterns and structures back to source events when detailed evidence is required.

Do not publish claims that this is already deployed, GPU-free, “for pennies,” or exceptionally inexpensive until workload, denominator, latency, resources, and comparison baseline are defined.

Do not revisit semantic search in the conclusion. The article must end on the production resolution.

---

## 16. Editorial Guardrails

### Terminology

- Use **CLP** consistently for the newer-generation system.
- Use **CLP-S** only when a historical source/name requires it.
- Use **Presto Native** for the product and **Presto Native worker** for compute.
- Use **schema-less** consistently in prose/headings.
- Use **complete event**, not “entire row,” for reconstructed JSON.
- Use **archive/split-local schema tree**, not dataset-global schema.

### Capability boundaries

- Current wildcard functions search values across unknown fields.
- Key discovery and general wildcard projection are future work.
- Production-only function syntax must be labeled and verified.
- Early termination is Uber production behavior, not current OSS.
- Mixed KV-IR/archive queries are current OSS.
- Offboarding, rehydration, dual ingest, and overlap precedence are Uber tier-management policies.
- In the Part III ingestion narrative, keep the metadata backend behind the generic **metastore** boundary.

### Claim strength

- Do not imply five workers scan 25 PB for every query.
- Do not claim perfect byte isolation.
- Do not claim universal immunity to ingestion loss.
- Do not claim all bursts compress unusually well.
- Do not call the object-storage path unlimited.
- Do not claim semantic search is deployed or benchmarked.
- Do not claim LLM context preloading or metadata-driven summarization is deployed or benchmarked.
- Do not call the public connector a preview.
- Do not call unshredded Variant opaque JSON.

### Article scope

- Keep the Uber article production-led and concise.
- Link **[PRESTO_BLOG_URL](PRESTO_BLOG_URL)** for the detailed Variant/shredding and connector discussion.
- Remove the placeholder before publication.
- Keep only one architecture figure, one SQL example, and one work-elimination figure.

---

## 17. Publication Checklist

- Replace every `PRESTO_BLOG_URL` placeholder with the published companion Presto article URL.
- Confirm the measurement window for 25 PB/month and 750–850 TB/month.
- Confirm 5–10% remains the approved description of typical worker CPU.
- Confirm 30-second-to-few-minute freshness remains current.
- Confirm publishable wording for rare hot/indexed-path loss and CLP-backed peak resilience.
- Confirm publishable wording for object-storage throughput headroom.
- Confirm offboarding, selective rehydration, and dual-ingest wording.
- Verify the exact public function/path syntax for the release used in the example.
- Verify the wildcard pattern used in the SQL example is supported by pushdown.
- Confirm QueryRunner responsibility wording.
- Confirm contributor names and organization attribution.
- Decide whether the double-digit-petabyte partner statement is approved; omit otherwise.
- Do not restore the incomplete performance table.
- Add a representative query only if every metric comes from one consistent definition.
- Validate Markdown, terminology, section budgets, and `git diff --check`.
- Do not commit unless explicitly requested.

---

## 18. Reference Index

### Uber series

- [Part I](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/)
- [Part II](https://www.uber.com/en-GB/blog/modernizing-logging-with-clp-ii/)

### CLP and research

- [CLP repository](https://github.com/y-scope/clp)
- [CLP Presto connector](https://github.com/y-scope/clp-plugin-presto-connector)
- [KV-IR design](https://docs.yscope.com/clp/main/dev-docs/design-kv-ir-streams/index.html)
- [KV-IR background](https://docs.yscope.com/clp/main/dev-docs/design-kv-ir-streams/background)
- [Using Presto with CLP](https://docs.yscope.com/clp/main/user-docs/guides-using-presto.html)
- [CLP connector documentation](https://docs.yscope.com/presto/connector/clp.html)
- [μSlope, OSDI '24](https://www.usenix.org/conference/osdi24/presentation/wang-rui)
- [Original CLP paper, OSDI '21](https://www.usenix.org/conference/osdi21/presentation/rodrigues)

### Presto infrastructure

- [Presto concepts](https://prestodb.io/docs/current/overview/concepts.html)
- [Presto Native / Presto C++](https://prestodb.io/docs/current/presto-cpp.html)
- [PR #27653](https://github.com/prestodb/presto/pull/27653)
- [PR #27652](https://github.com/prestodb/presto/pull/27652)

### Uber query ecosystem

- [DataCentral](https://www.uber.com/en-CA/blog/datacentral-ubers-observability-and-chargeback-platform/)
- [Presto Express](https://www.uber.com/blog/presto-express/)
- [Preon](https://www.uber.com/en-DE/blog/preon/)
- [Engineering SQL Support on Apache Pinot at Uber](https://www.uber.com/en-GB/blog/engineering-sql-support-on-apache-pinot/)
- [Serving Millions of Apache Pinot Queries with Neutrino](https://www.uber.com/en-US/blog/serving-millions-of-apache-pinot-queries-with-neutrino/)

---

## 19. Working Abstract

Uber and YScope expanded CLP from its original Spark deployment to machine-learning/AI, streaming, and data platforms that collectively generate approximately 25 PB of raw logs per month. A collector on every host monitors and enriches container logs, incrementally compresses them into an appendable KV-IR file, periodically uploads the growing compressed file without closing it, and publishes metadata updates through Kafka. Fresh logs remain searchable in KV-IR before asynchronous aggregation transforms them into denser immutable CLP archives, and one open-source connector query can span both formats without duplicate events. Within each archive, CLP groups recurring key-and-type schemas into encoded tables and columnizes every schema node. The Presto Native connector rewrites supported SQL expressions, resolves paths against split-local structure, and pushes filtering and projection into CLP's encoded execution. Metadata pruning and Uber's planning-time early termination avoid files and splits before object-storage reads begin. In production, CLP reduces the monthly raw volume to approximately 750–850 TB, while five lightly utilized Presto Native workers serve recent and older data directly from Uber's internal object storage without local staging or an archive cache.
