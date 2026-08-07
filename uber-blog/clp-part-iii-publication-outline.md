# From 25 PB of Logs to Native SQL: CLP Part III Publication Outline

## Editorial Target

- **Recommended title:** *From 25 PB of Logs to Native SQL: Modernizing Logging at Uber with CLP (Part III)*
- **Alternative title:** *Bringing Native Schemaless Log Analytics to Presto at Uber*
- **Target length:** 2,400–2,700 words
- **Hard maximum:** 3,000 words
- **Figures:** Two
- **Code examples:** One compact SQL example
- **Central protagonists:** The Uber–YScope engineering collaboration
- **Central technical idea:** Schemaless logs do not need to be flattened, indexed exhaustively, or hidden inside an opaque JSON string to become first-class SQL data.
- **Central proof:** Approximately 25 PB of raw logs per month are compressed to 750–850 TB and queried by five lightly utilized Presto-native workers.

The existing [Part I](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) and [Part II](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) are already substantial by Uber Engineering standards. Part III should be shorter and more focused while retaining enough technical substance to be intellectually satisfying.

## Story Summary

Uber's logs have outgrown the old division between unstructured messages and structured JSON. They now form an evolving spectrum in which fields appear, disappear, change type, and coexist with natural language. CLP-S can compress and search that spectrum efficiently, but storage efficiency alone does not integrate the data with Uber's SQL-centered observability ecosystem.

Uber and YScope therefore brought CLP's schema-less query mechanics into Presto Native. Common discovered fields remain ordinary columns, while UDF expressions carry dynamic field access, filtering, projection, and wildcard search through SQL. The connector rewrites supported expressions and pushes them into CLP-S rather than reconstructing every event as raw JSON.

The small production footprint comes from two complementary effects. Metadata pruning and planning-time early termination avoid archives and splits; CLP-S then searches the remaining encoded data quickly as bytes arrive from object storage. Together, these mechanisms explain how approximately 25 PB of raw logs per month can compress to 750–850 TB and be served by five lightly utilized Presto-native workers without local staging or an archive cache.

### Narrative Chain

1. **Create the mystery:** Open with 25 PB/month, 29–33× compression, five workers, and direct object-storage search.
2. **Define the data problem:** Logs span natural-language messages, enriched events, and polymorphic JSON.
3. **Create the SQL tension:** Neither predefined columns nor one opaque JSON value preserves the required expressiveness and execution model.
4. **Place the solution:** QueryRunner routes users to Pinot for hot access and Presto Native plus CLP-S for warm/cold access.
5. **Reveal schema-less SQL:** UDFs express dynamic operations and are rewritten into native CLP-S filtering and projection.
6. **Explain the footprint:** Metadata avoids work; CLP-S makes the remaining work inexpensive.
7. **Prove it in production:** Return to the opening figures and distinguish public functionality from Uber-specific extensions.
8. **Land the lesson:** Schema-less does not have to mean opaque, and petabyte-scale logs do not require a petabyte-scale query cluster.

### Reader Progression

- **Surprise:** How can five workers serve this scale?
- **Recognition:** Real logs do not have stable schemas.
- **Insight:** SQL carries log-native mechanics instead of forcing logs into a conventional table.
- **Deeper insight:** Work avoidance and fast encoded execution reinforce each other.
- **Proof:** Production measurements explain the small compute footprint.
- **Takeaway:** The query interface can preserve the nature of the data rather than erase it.

## Desired Tone

- Technically confident, precise, and accessible.
- Build awe through scale and mechanism, not adjectives.
- Explicitly present the work as a sustained collaboration between Uber and YScope.
- Introduce technical concepts through concrete log examples before naming the abstractions.
- Avoid claims such as “revolutionary,” “unprecedented,” or “impossible” unless supported by direct comparisons.
- Let readers first think, “SQL cannot normally do this,” and then show how the collaboration changed the boundaries between SQL, query planning, and the storage format.
- Maintain intellectual honesty:
  - Distinguish raw from compressed volume.
  - Distinguish the open-source connector from Uber’s more advanced production implementation.
  - Describe future semantic-search capabilities as future work.
  - Do not imply that five workers scan the entire 25 PB for every query.

---

## Introduction: The Scale–Simplicity Paradox

**Target: 180–220 words**

### Opening Move

- Open immediately with the production contrast:
  - Uber’s participating ML, streaming, and data platforms generate approximately **25 PB of raw logs per month**.
  - CLP compresses that volume to approximately **750–850 TB**.
  - The warm and cold search workload is served by only **five Presto-native workers**, each with 48 cores.
  - These workers typically consume only around **5–10% CPU**, while query latency remains fast enough for a responsive user experience.
  - Uber intentionally uses object-storage API rate limits to pace queries at the lowest practical resource consumption that still satisfies the user-experience objective.
  - The design target is sufficient latency with minimal use of shared infrastructure—not maximum query throughput.
- Present the surprising user experience:
  - Engineers query this changing, polymorphic log data through SQL.
  - The data remains in the CLP-S format in object storage.
  - Workers search archives directly from object storage, without local-disk staging or an archive cache in the query path.
  - Supported searches and projections execute inside the storage layer instead of deserializing every record through Presto.

### Series Continuity

- Summarize the previous articles in no more than three sentences:
  - [Part I](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) introduced streaming CLP compression for Spark and its impact on cost, retention, and SSD durability.
  - [Part II](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) expanded the system into an end-to-end platform for ingesting, retaining, viewing, and programmatically analyzing logs.
  - Part III asks the next question: **How can this rapidly evolving data become a native part of Uber’s SQL ecosystem without giving up CLP’s compression and search performance?**

### Collaboration Statement

- Establish attribution in the introduction:
  - This capability is the result of a long-running collaboration between Uber and YScope.
  - Uber and YScope engineers jointly expanded CLP across Uber platforms, developed the logging integrations, designed the production query architecture, and brought CLP’s schemaless execution model into Presto-native.
  - YScope engineers were the primary contributors to the open-source CLP connector, working alongside Uber engineers and the broader Presto community.

### Suggested Final Sentence

> The result is a system in which SQL provides the familiar interface, while CLP-S and its metadata layer perform the log-native work that conventional SQL engines and JSON storage cannot do efficiently.

---

## 1. Logs No Longer Fit into “Structured” and “Unstructured” Boxes

**Target: 300–350 words**

### Expansion Beyond Spark

- CLP began with Spark but now supports logs from major Uber platform families:
  - ML and AI platforms, including Michelangelo and Ray
  - Streaming platforms, including Flink and Kafka
  - Traditional data platforms, including Spark and Hudi
- Avoid claiming that thousands of Uber services have already been migrated.
- Describe the present deployment as a substantial set of large platform services producing double-digit petabytes of logs.
- Position broader service onboarding as future scaling work.

### Joint Logging Integrations

- Uber and YScope developed language-specific logging appenders and native bindings.
- These integrations enrich existing logs with fields such as:
  - Correlation identifiers
  - Platform, application, and job identifiers
  - Host and container identifiers
  - Time, log level, and other execution context
- Existing pattern-string configurations are automatically translated into JSON fields.
- Application owners generally do not need to rewrite those pattern configurations or change individual logging statements.
- This is a key adoption point: richer records are produced at the platform boundary rather than through broad application rewrites.

### The Structure Continuum

- Explain that the traditional boundary has blurred:
  - A message-oriented log is dominated by natural-language text with embedded variables.
  - A conventional JSON event is dominated by request data, state, and dimensions, but often contains a natural-language description.
  - Enriched message logs combine both forms.
- CLP-S supports this full spectrum rather than forcing it into separate storage models.
- When introducing CLP-S, link to the [OSDI ’24 μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui) and identify it as joint work by YScope, the University of Toronto, and Uber’s Observability, Pinot, and Storage and Search teams.

### Define the Two Important Concepts

- **Semi-structured logs:** Records have discoverable fields, but those fields are not governed by one complete, static schema.
- **Type polymorphism:** The same field path may contain different types in different records—for example, an identifier may appear as an integer in one producer, a string in another, and an object in a third.
- Explain why these properties are natural:
  - Services deploy independently.
  - Logging libraries and languages differ.
  - Fields are added, removed, and repurposed.
  - Historical and current records coexist.

### Section Takeaway

> At logging scale, schema evolution is not an occasional migration event. It is a continuous property of the data.

---

## 2. When SQL Meets Schemaless Logs

**Target: 300–350 words**

### Presto’s Normal Contract

- Presto normally expects columns and their types to be known at query-planning time.
- Schemaless logs violate that expectation:
  - Fields appear and disappear.
  - Nested structures evolve.
  - A field can have multiple types.
  - Field names may contain special characters, differ only by case, or conflict with SQL keywords.

### Why Manual Schema Management Fails

- No central team can continuously predefine every field emitted by independent applications.
- Selecting one “canonical” type either loses information or breaks queries when another type appears.
- Continually rebuilding schemas and tables would turn ordinary logging changes into data-platform migrations.

### Why One JSON String Is Not an Acceptable Escape Hatch

- Storing every event as one opaque JSON column technically avoids schema planning.
- Operationally, it requires repeatedly:
  - Reading entire records
  - Parsing and materializing JSON
  - Discovering types at execution time
  - Throwing away most of the decoded data after applying a filter
- This sacrifices projection, pruning, compressed execution, and much of the performance advantage CLP-S provides.

### SQL’s Expressiveness Gap

- Standard SQL also lacks natural ways to express some log operations:
  - Search only field values without knowing every field name.
  - Search only field names.
  - Search both keys and values.
  - Apply grep-like wildcard or regular-expression semantics to message-oriented data.
  - Select one physical type of a polymorphic field—or intentionally select all its types.
  - Safely address dynamically discovered fields with unusual names.

### Why Presto Still Matters

- Presto is more than a SQL compatibility layer.
- It connects CLP to Uber’s established query ecosystem:
  - Observability user interfaces
  - Platform teams’ SQL query builders
  - Programmatic queries and automation
  - QueryRunner’s routing, cluster selection, policy, and failover
  - Projection, aggregation, joins, and existing analytical tools

### Transition Question

> Could Uber preserve this SQL ecosystem while allowing CLP-S—not a generic JSON parser—to interpret and search the underlying logs?

---

## 3. One Query Entry Point, Multiple Storage Tiers

**Target: 220–280 words**

### Figure 1: Uber Log-Query Architecture

```text
Observability UI       Platform Query Builder       Programmatic Clients
         \                       |                         /
                           QueryRunner
               SQL parsing, policy, routing, cluster
                     selection, and failover
                              |
                +-------------+-------------+
                |                           |
          Pinot: hot logs           Presto-native + CLP
          indexed access              warm/cold logs
                |                           |
          streaming data       Metastore + CLP-S archives
                                in object storage
```

### Accompanying Points

- Users enter through familiar interfaces rather than selecting storage engines directly.
- QueryRunner interprets and routes SQL to the appropriate backend.
- Pinot serves hot logs that benefit from indexed random access.
- Presto-native and CLP-S serve warm and cold logs from object storage.
- Workers stream and search CLP-S archives directly; they do not first copy them to local data disks, and execution does not depend on an archive cache.
- Search begins as compressed bytes arrive from object storage.
- CLP-S does not reconstruct archives into raw JSON/text and then scan them.
- Its schema tree, log-type dictionaries, and statistics identify encoded regions that could contain matches.
- Only candidate regions are selectively decoded or decompressed, and only matching events and requested values are materialized.
- “Warm and cold” does not mean closed-file-only access:
  - Active log streams are published incrementally.
  - Their latest data becomes queryable through Presto before the producer closes the file.
  - Typical freshness is approximately 30 seconds to a few minutes, which is sufficient for current users of this tier.
- Frame publication as a balance among:
  - Compression ratio: larger chunks generally compress better.
  - Data freshness: smaller, more frequent publications reduce visibility delay.
  - Cost: frequent object-storage writes and API calls consume more shared infrastructure.
- CLP’s compressed format is optimized for streaming scans, coarse-grained skipping, and high compression.

### Pinot Scope Boundary

- Include only one forward-looking sentence:
  - The hot tier reuses both CLP-S ideas and implementation components. Uber and YScope ported and adapted dynamic schema discovery and maintenance, custom tokenization, and grep-like search algorithms to Pinot-native structures and Lucene indexes optimized for low-latency random access. The details are outside this article’s scope and may be discussed through a future venue.
- Do not explain Pinot’s Lucene token representation, schema initialization, schema evolution, or grep-like execution here.

---

## 4. Bringing Native Schemaless Data Access to Presto

**Target: 550–650 words**

This is the article’s central technical section.

### Joint Contribution

- State clearly:
  - YScope engineers, working alongside Uber engineers, developed a Presto-native CLP connector and integrated it into Uber’s production query environment.
  - The work combines CLP-S’s schema discovery and compressed search with Presto’s planning and distributed execution.
  - It required both a connector and extensions to how log-specific operations are represented and pushed down.

### Dynamic Schema Exposure

- CLP-S maintains a schema tree discovered from the records themselves.
- The connector presents dynamically discovered fields to Presto without requiring one manually maintained schema.
- The schema tree records the field paths and types observed across the dataset; it does not impose one structural schema on every log event.
- Individual records may retain different schemas.
- The access model handles:
  - Nested fields
  - Fields added or removed over time
  - Case-sensitive names
  - Special characters and SQL keyword collisions
  - Multiple physical types at the same field path
- Users can address a specific type or request values across all available types.
- Users can project discovered fields individually or retrieve a complete object as JSON text.
- Missing fields use `NULL` semantics for the requested projection.
- State explicitly that current production access does not materialize complete dynamic objects using Presto’s `ROW` data type.
- Supporting the `ROW` data type for such objects is a possible future extension.

### Schemaless Access UDFs

- Users can project individual discovered fields rather than decoding an entire JSON object.
- Present the UDF family as the natural SQL representation of schemaless query mechanics, not as conventional record-at-a-time functions.
- Explain that projection and search/filtering are not strictly separate UDF categories:
  - The same typed expression can project a field in `SELECT`.
  - It can constrain that field in `WHERE`.
  - One query may use it for both.
- Name the typed access UDF family rather than discussing projection only conceptually:
  - `CLP_GET_INT(<json-path>)`
  - `CLP_GET_FLOAT(<json-path>)`
  - `CLP_GET_STRING(<json-path>)`
  - `CLP_GET_BOOL(<json-path>)`
  - `CLP_GET_JSON_STRING()` for complete-event reconstruction
- Explain that typed UDFs allow users to select the desired variant of a polymorphic field without coercing every value into one type.
- Log-aware SQL functions express operations that ordinary SQL cannot naturally represent:
  - Key-only search
  - Value-only search
  - Combined key/value search
  - Message searches
  - Grep-like wildcard and regular-expression behavior
  - Polymorphic field access
- Include `CLP_WILDCARD_COLUMN()` as the RFC example for searching across dynamically discovered fields.
- Explain the execution path: the connector recognizes these expressions during planning, rewrites the query, and pushes native schemaless field access, projection, and filtering into CLP-S.

### One Compact SQL Example

- Show a representative query containing:
  - A time range
  - A platform or application identifier
  - Typed projection through `CLP_GET_STRING(...)` or `CLP_GET_INT(...)`
  - A predicate using the same `CLP_GET_INT(...)` expression
  - Search across dynamic fields through `CLP_WILDCARD_COLUMN()`
  - `ORDER BY timestamp DESC`
  - `LIMIT`
- Keep it to approximately 8–12 lines.
- Annotate no more than three features.
- Validate final function names and syntax against the production implementation before publication.

### Native Execution Rather Than Cosmetic SQL

- This is not merely a collection of record-at-a-time JSON UDFs.
- During planning, the connector recognizes supported expressions and translates them into CLP-native operations.
- Push down:
  - Time and top-level metadata filters
  - Log-aware search predicates
  - Dynamic field projection
  - Other supported constraints
- CLP-S evaluates these operations using its schema tree, dictionaries, statistics, and compressed representation.
- Presto receives only the matching events and projected values needed by the remaining query.

### Core Takeaway

> SQL supplies the composition model; CLP-S supplies the semantics and physical execution required for schemaless logs.

### Presto-Native Ecosystem Contribution

- Keep this to one concise paragraph or callout:
  - Supporting the connector also helped close an upstream Presto-native infrastructure gap around dynamically loaded, out-of-band C++ connectors.
  - YScope contributors ChenXing Yang and Jack Luo led the relevant connector-enablement work.
  - IBM engineers—including Deepak Majeti—provided infrastructure contributions and review.
  - Meta and the Presto Foundation provided design feedback and helped drive the upstream effort.
- Avoid allowing the contributor history to interrupt the main system narrative.

---

## 5. Avoiding Work Is More Important Than Adding Workers

**Target: 450–550 words**

This section explains the surprising production footprint.

### The Metastore Is Part of the Query Engine

- Petabyte-scale object storage alone is not enough.
- Before assigning Presto splits, the system must identify which compressed files could possibly satisfy the query.
- The connector does not depend on one specific metastore implementation.
- A suitable metastore should support:
  - Billions of files
  - Time ranges and top-level fields
  - Fields being added or removed
  - Streaming or incremental metadata results
  - Transactional consistency where practical, although eventual consistency can also work

### Coarse-Grained Pruning

- Use metadata such as:
  - Time range
  - Platform, application, job, host, or correlation identifiers
  - Available top-level fields
  - File-level statistics
  - CLP schema and dictionary metadata
- Prune irrelevant files before object-storage reads and worker scheduling.
- Describe this as the first form of avoiding work.

### Planning-Time Early Termination

- Begin with the familiar log-query shape:

```sql
ORDER BY timestamp DESC
LIMIT N
```

- Logs are append-only and partitioned primarily by time.
- Individual compressed files are timestamp-sorted.
- Collectively, the archive is therefore a set of overlapping, sorted chunks.

### The Presto Challenge

- Presto does not normally guarantee split execution order.
- It also does not normally stop an entire distributed query merely because enough globally ordered results have already been found.
- Scheduling every potentially relevant file would waste object-store bandwidth for a query asking only for the latest few matching records.

### The Optimization

- The metastore streams metadata in timestamp-aware order.
- Query planning progressively selects files.
- Once the planner has enough coverage to prove that older files cannot change the requested Top-N result, it stops producing additional splits.
- This is planning-time early termination, not ordinary execution-time cancellation.
- Pruning and early termination are independent but composable:
  - A query may combine top-level application, host, job, or time filters with `ORDER BY timestamp DESC LIMIT N`.
  - The metastore first applies top-level filters to prune files that cannot match.
  - Early termination then streams the surviving metadata in timestamp order.
  - It stops when older surviving files cannot change the filtered Top-N result.

### Figure 2: Query-Work Elimination

```text
SQL predicates
      |
Metadata pruning
(remove impossible files)
      |
Timestamp-aware planning
(stop after sufficient Top-N coverage)
      |
CLP-native predicate and projection pushdown
      |
Streaming search over encoded object-storage data
(no local staging/cache; selective candidate-region decode)
      |
Small result returned to Presto
```

### Section Takeaway

> The architecture scales by proving which data does not need to be read, then executing the remaining search inside the compressed format.

---

## 6. Production Impact and Open-Source Ecosystem

**Target: 250–300 words**

### Only Use Numbers Supporting the Story

- Approximately **25 PB of raw logs per month**
- Approximately **750–850 TB after CLP compression**
- Equivalent to roughly:
  - **29–33× compression**
  - About **97% less storage**
- Query service:
  - **Five Presto-native workers**
  - **48 cores per worker**
  - Typically **5–10% CPU utilization**
  - Direct object-storage search with no local data disks or archive cache in the query path
  - Deliberately paced by internal object-storage API rate limits to minimize consumption of shared infrastructure
  - Already fast enough for a responsive user experience; additional throughput, worker capacity, or CPU optimization would not materially improve that experience
- Data freshness:
  - Open log streams become queryable before their files close.
  - New events are typically visible within approximately 30 seconds to a few minutes.
  - This range is sufficient for current users; do not request or promise percentile statistics.

### Optional Additional Metric

Include only one of the following if a defensible production measurement is available:

- Median and P95 latency for a representative interactive search, or
- Percentage of candidate files eliminated by metadata pruning, splits avoided through early termination, compressed gigabytes read, and object-storage API requests avoided for one consistently defined representative query

- Label gigabytes as physical compressed data read from object storage, not logical raw log volume.

Do not add generic active-user or query-count statistics unless they directly demonstrate adoption or performance.

### Open Source

- Link to the open-source [CLP Presto connector](https://github.com/y-scope/clp-plugin-presto-connector).
- Link to the main [CLP repository](https://github.com/y-scope/clp), where users can compress logs and query them through Presto.
- Explain accurately:
  - The public connector is a usable reference implementation.
  - Uber and YScope’s private production implementation contains more advanced metastore, planning, and optimization capabilities.
  - The collaboration intends to open-source additional components where practical.
- Mention that the connector is also being used with YScope partners operating log volumes in the double-digit-petabyte-per-month range.
- Do not call them “customers” unless approved.

---

## Conclusion: Schemaless Does Not Have to Mean Opaque

**Target: 150–200 words**

### Recap

- Uber and YScope expanded CLP from Spark into major ML, streaming, and data platforms.
- Language-specific appenders enrich existing logs without requiring teams to rewrite their logging configurations.
- CLP-S handles the continuum from natural-language messages to deeply nested JSON, including schema evolution and polymorphic types.
- The Presto-native connector makes that data accessible through Uber’s established SQL query stack.
- Metastore pruning, planning-time early termination, and CLP-native pushdown make the architecture efficient on object storage.
- The production footprint demonstrates that petabyte-scale log analytics does not inherently require a correspondingly enormous query cluster.

### Recommended Closing Thought

> The most important result of the Uber–YScope collaboration is not simply that CLP-compressed logs can be queried with SQL. It is that logs can retain their natural, evolving structure—and still receive the projection, search, optimization, and composability expected from a modern analytical system.

### Forward-Looking Teaser

- The adaptation of CLP-S concepts and implementation components for Pinot’s random-access hot tier may be discussed through a future venue; do not promise a specific article or channel.
- Deeper Presto-native connector and query-language details may be shared through a future venue; do not promise a specific article or channel.
- CLP-S internals—including its schema tree, dictionaries, statistics, summaries, and emerging semantic-search capabilities—may likewise be discussed elsewhere in the future.
- Direct object-storage search makes local or edge execution a possible future direction: for suitable authorized investigations, CLP-S compute could run on an engineer's laptop or another edge environment instead of always using centralized Presto workers.
- Present local/edge execution as an architectural possibility subject to access control and resource governance, not a committed product or publication roadmap.

---

## Explicitly Exclude from Part III

- Detailed history of SSD burnout and the original two-phase appender.
- A second explanation of CLP’s fundamental compression algorithm.
- Full Pinot schema discovery, Lucene tokenization, or search design.
- QueryRunner internals beyond its role in the architecture.
- Exact metastore database implementation details.
- A complete proof or pseudocode for early termination.
- Exhaustive UDF documentation.
- Detailed semantic-search internals or “for pennies” claims.
- Every production or adoption metric available.
- Lengthy organizational or contributor history.
- Generic claims about thousands of already-onboarded services.
