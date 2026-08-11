# From 25 PB of Logs to Native SQL: CLP Part III Publication Outline

- **Status:** Revised around the end-to-end ingestion, connector, and search story
- **Audience:** Infrastructure, observability, storage, data-platform, streaming, and ML/AI engineers; engineering leaders evaluating cost and operability; Presto and CLP users interested in a production deployment
- **Target length:** 2,200–2,600 words; 3,000-word hard maximum
- **Article type:** Production architecture case study

## Story at a Glance

The article follows logs from independent producers to a SQL result. Readers should leave with three connected takeaways:

1. **Ingestion:** CLP accepts evolving, polymorphic logs without requiring a global producer schema. KV-IR compresses events incrementally for freshness; asynchronous aggregation transforms those streams into denser long-term archives.
2. **SQL integration:** The Presto Native connector exposes CLP data through Uber’s existing SQL ecosystem without first expanding every event into row-oriented JSON.
3. **Search:** Metadata planning, split-local resolution, and encoded execution eliminate enough work that five lightly utilized workers can serve the currently onboarded corpus.

The causal spine is:

> Many independently evolving producers create a schema-coordination problem. CLP preserves their structure during ingestion. The Presto connector carries SQL intent into that storage model. Metadata and encoded execution then avoid enough work to keep the query tier small.

This is broader than the previous query-serving outline. Ingestion is now a first-class architectural contribution, but every section must still advance the same producer-to-query story.

## Scope and Claim Boundaries

### Supported production facts

- Currently onboarded ML/AI, streaming, and data-platform workloads produce approximately 25 PB of raw logs per month.
- CLP reduces that volume to approximately 750–850 TB per month: roughly 29–33× compression and about 97% less storage.
- Five 48-core Presto Native workers serve recent and older CLP data directly from Uber’s internal object storage, typically at approximately 5–10% CPU.
- Recent events generally become searchable through KV-IR in approximately 30 seconds to a few minutes.
- Uber’s metadata implementation manages billions of file records.

### Capability labels

- **Current OSS:** Public connector and CLP behavior verifiable in code or documentation.
- **Uber production:** Deployed extensions or policies that are not necessarily public.
- **Future direction:** Compatible design work that is neither deployed nor promised.

## Narrative Map

| Section | Reader question | Main contribution |
|---|---|---|
| Opening | How can a five-worker query tier serve this production volume? | Establishes the scale and previews ingestion, integration, and work avoidance as one lifecycle. |
| 1. Evolving logs | Why can’t producers share a conventional table schema? | Defines semi-structured data, polymorphism, and the adoption constraint. |
| 2. Ingestion | How do fresh events become searchable without blocking on archive construction? | Makes ingestion a co-equal pillar and explains KV-IR plus asynchronous aggregation. |
| 3. SQL entry point | How do users query recent and archived representations without choosing one? | Places Presto Native and CLP inside Uber’s routed query ecosystem. |
| 4. Native SQL | How does a SQL expression reach CLP’s encoded structure? | Shows that the connector preserves storage-native execution instead of wrapping JSON parsing. |
| 5. Search | Why does most of the corpus never reach a worker? | Explains metadata pruning, safe Top-N early termination, split-local resolution, and encoded search. |
| 6. Impact and OSS | What is measured, public, Uber-specific, and collaborative? | Closes the evidence loop and states capability boundaries. |
| Conclusion | What is the transferable system lesson? | Reconnects producer flexibility, SQL usability, and economical search. |

## Opening: Follow the Log Lifecycle

**Target:** 170–210 words

### Goal

Create the production question without implying that every query scans 25 PB.

### Important points

- Contrast 25 PB/month raw, 750–850 TB/month compressed, and five 48-core workers.
- State that this is monthly ingest from onboarded workloads, not per-query scan volume.
- Mention direct object-storage access without local staging or an archive cache.
- Preview the three-part answer: schema-independent ingestion, storage-aware SQL integration, and search work elimination.

### Takeaway

Compression is necessary, but the small query tier makes sense only when the full lifecycle is considered.

## 1. Evolving Logs Without a Global Schema

**Target:** 300–360 words

### Goal

Show why independently deployed producers make schema evolution continuous rather than an occasional table migration.

### Important points

- Summarize Parts I and II in no more than three sentences.
- Show expansion into representative ML/AI, streaming, and data platforms without implying all Uber services are onboarded.
- Explain how appenders and native bindings preserve messages while adding correlation, platform, application, job, host, and container context.
- Define **semi-structured** and **polymorphic** after giving concrete message and JSON examples.
- Explain why a central table schema creates coordination and why eager whole-document JSON processing loses selective execution.
- Link the μSlope paper for the storage research.

### Takeaway

The platform must preserve producer-controlled structure without turning application deployments into data-platform migrations.

## 2. Ingestion Without Producer Coordination

**Target:** 240–300 words, plus one lifecycle figure

### Goal

Make ingestion a first-class systems contribution and explain why the design remains simple as producer diversity grows.

### Important points

- A log collector runs on every host and monitors logs from its containers.
- The collector enriches events and incrementally compresses them into an appendable KV-IR file; the active file is already compressed as it grows.
- On a configurable timeout, it uploads the current file extent without closing the stream and continues appending later events.
- Each file update produces a metadata event on Kafka; the metastore consumes these updates for query discovery.
- Fresh events become searchable in low-buffering, streaming-compressed KV-IR files.
- Asynchronous aggregation later transforms the already compressed KV-IR inputs into denser immutable CLP archives; it is not where compression begins.
- Explain the cadence tradeoff among freshness, compression, and object-storage request cost.
- Current approved freshness is approximately 30 seconds to a few minutes.
- The stable contract is structure preservation; producers do not need identical event shapes.

### Figure 1: Producer-to-storage lifecycle

The figure should show:

- Container logs on every host
- Host-local monitoring and enrichment
- Appendable streaming KV-IR compression inside the collector
- Configurable uploads without closing the active file
- Metadata updates through Kafka into the metastore
- Searchable KV-IR in Uber’s internal object storage
- Asynchronous archive aggregation

Treat the metadata implementation as the **metastore**. Pinot is not part of the ingestion explanation and should not appear in the figure.

### Takeaway

Freshness and dense long-term storage operate at different cadences, allowing a simple producer contract without sacrificing searchability.

## 3. One SQL Entry Point Across Recent and Archived Data

**Target:** 230–280 words

### Goal

Place CLP inside the system users experience and explain why SQL clients do not select a storage engine or file format.

### Important points

- Existing observability interfaces, query builders, and programmatic clients use Uber’s Presto-oriented ecosystem.
- QueryRunner and related services participate in governance, routing, cluster selection, and failover.
- For CLP data, the metastore identifies relevant KV-IR files and archives; Presto Native executes the resulting splits.
- Treat the metastore backend as a black box whose planning contract matters more than its implementation.
- One CLP query may contain both KV-IR and archive splits.
- During aggregation overlap, the archived representation takes precedence only over its incorporated KV-IR inputs, so each event appears once.
- End by explaining why a naive JSON compatibility layer would expand encoded data before filtering.

### Takeaway

Users retain one SQL experience while the platform selects physical storage and representation.

## 4. From Polymorphic Logs to Native SQL

**Target:** 400–470 words, including one SQL example

### Goal

Prove that the connector carries SQL intent into CLP rather than evaluating record-at-a-time UDFs over reconstructed JSON.

### Important points

- Each archive has an archive-local schema tree and schema-homogeneous encoded tables.
- Records with the same keys and value types share a table; every observed schema node is columnized within that table.
- One logical path can bind to multiple typed columns without creating one global sparse table.
- CLP separates recurring natural-language patterns from typed variables.
- An optional convenience schema is a late-bound view over existing columns, not a rewrite.
- Typed accessors preserve polymorphism: `CLP_GET_STRING`, `CLP_GET_BIGINT`, and other verified public functions.
- `CLP_WILDCARD_*_COLUMN()` searches typed values across fields when the key is unknown.
- `CLP_GET_JSON_STRING()` reconstructs a complete event when required.
- Use one query containing exact projection, an exact predicate, typed wildcard value search, a time/application filter, and `ORDER BY timestamp DESC LIMIT 100`.

Use this compact execution chain:

> **SQL expression → connector rewrite and pushdown → split-local schema resolution → encoded CLP execution**

- The Java function bodies are planner markers.
- Unsupported residual expressions remain in Presto.
- A Presto split is the unit assigned to a worker; resolution happens against each split’s local structure.
- The connector runs in-process in Presto Native and invokes CLP’s native reader.
- For archive splits, CLP searches compressed bytes from the object-storage stream without staging the complete archive.
- Link the companion Presto article instead of importing its complete Variant/shredding or plugin-development discussions.

### Takeaway

SQL supplies relational composition; CLP supplies semi-structured semantics and encoded physical execution.

## 5. Search by Eliminating Work

**Target:** 340–420 words, plus one work-elimination figure

### Goal

Resolve the five-worker question through specific mechanisms rather than an unexplained performance claim.

### Important points

1. **File pruning:** The metastore uses time bounds, record counts, selected dimensions, available paths, and file-level statistics to remove impossible files before split scheduling.
2. **Planning-time early termination:** For eligible newest-first limited queries, stop generating older splits only after proving that they cannot affect the result.
3. **Split-local resolution:** Narrow each selected archive to relevant encoded tables and columns.
4. **Encoded execution:** Apply supported filtering and projection before reconstructing requested values or complete events.

Keep pruning and early termination distinct:

- Pruning proves a file cannot match a predicate.
- Early termination proves an older candidate may match but cannot change an eligible global Top-N result.
- Required conditions include append-only logs, trustworthy time bounds and counts, time-aware enumeration, a finite limit, and compatible filters.
- If correctness cannot be proven, plan more files or use the complete candidate set.
- The optimization changes which splits exist; it does not require ordered worker execution.

Explain that archive metadata appears early and the reader can advance without backward seeks. Avoid claiming perfect byte isolation or zero object-store overhead.

### Figure 2: Work elimination

```text
SQL predicates and Top-N
          |
file-level metadata pruning
          |
correctness-preserving split early termination
          |
split-local table and column resolution
          |
streaming encoded CLP search
          |
small result returned to Presto
```

### Takeaway

The connector’s value is not SQL syntax alone; it preserves opportunities to avoid I/O, decompression, parsing, and materialization at the appropriate layer.

## 6. Production Impact, Open Source, and Collaboration

**Target:** 280–340 words

### Goal

Close the evidence loop and separate public capabilities from Uber extensions.

### Important points

- Collect the approved storage, worker, utilization, and freshness figures without repeating the full opening.
- Explain that object-storage access is deliberately paced because responsive latency, not maximum throughput, is the goal.

**Current OSS:** mixed KV-IR/archive queries, typed exact paths, expression rewriting, filter/projection pushdown, typed wildcard value search across unknown keys, complete-event JSON, and pluggable metadata/split filtering.

**Uber production:** advanced metastore, billion-file scale, planning-time early termination, routing, reliability, and operational controls.

End with attribution:

- YScope engineers led connector implementation.
- IBM engineers and the Presto community supported native connector infrastructure and review.
- YScope and Uber engineers extended and applied the architecture in production.

### Takeaway

Readers should know both why the deployment works and which parts they can use from open source today.

## Conclusion

**Target:** 130–170 words

### Goal

Compress the article into one transferable system principle.

### Important points

- Revisit the three connected requirements: producer independence, familiar SQL, and economical search.
- Recap KV-IR plus archive aggregation, storage-aware connector execution, and work elimination.
- Do not introduce resilience, LLM/semantic-search, edge execution, or another roadmap after the production evidence.

### Closing idea

> Semi-structured logs can remain faithful to their producers, become part of an analytical SQL ecosystem, and still be served economically at production scale.

## Publication Guardrails

- Use **CLP**, not CLP-S, except in historically necessary quotations.
- Scope 25 PB/month to currently onboarded workloads; never imply one query scans that volume.
- Keep raw logical volume, compressed footprint, and physical bytes read distinct.
- Scope the deployment to currently onboarded ML/AI, streaming, and data platforms; do not call it Uber’s entire logging infrastructure.
- Describe the authoritative schema as archive/split-local, not dataset-global.
- Distinguish current typed wildcard **value search across unknown keys** from future key discovery or general `{path, value}` projection.
- Do not describe planner-marker functions as row-at-a-time UDF execution.
- Preserve ownership: Presto Native supplies workers; the connector runs in-process; CLP interprets and searches storage bytes.
- Distinguish metadata pruning from correctness-preserving early termination.
- In the ingestion story, describe Pinot only through the generic **metastore** boundary; it is not a separate architectural pillar.
- Separate current OSS, Uber production extensions, and future work at first mention.
- Avoid “full CLP speed,” “data explosion,” or universal sub-second/few-second claims without a controlled definition and evidence.
- Keep contributor attribution at the end rather than interrupting the architecture narrative.

## Out of Scope

- Complete Iceberg Variant or Parquet shredding comparison
- Full CLP format or compression-algorithm derivation
- Presto Native connector packaging and development guide
- QueryRunner internals beyond their role in the serving path
- Formal early-termination proof or pseudocode
- Exhaustive SQL function documentation
- Incomplete benchmark dashboards
- Detailed future semantic-search or LLM architecture
