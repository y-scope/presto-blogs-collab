# Making Petabyte-Scale Semi-Structured Logs SQL-Native: Inside Uber’s CLP and Presto Infrastructure (Part III)

**Document status:** Working publication draft

At Uber, the [Compressed Log Processor (CLP) platform](https://github.com/y-scope/clp) now handles logs from machine learning and AI platforms such as Michelangelo and Ray, streaming platforms such as Flink and Kafka, and data platforms such as Hudi and Spark. Together, these workloads produce approximately 25 PB of raw semi-structured logs each month. CLP reduces that volume to 750–850 TB. Yet five 48-core [Presto Native](https://prestodb.io/docs/current/presto-cpp.html) workers—Presto's C++ worker implementation—query this data directly from Uber's internal object storage, typically at 5–10% CPU utilization and without staging or caching complete archives. How can such a small query cluster provide SQL access at that scale?

This is the third blog in Uber's CLP series. [“Reducing Logging Cost by Two Orders of Magnitude Using CLP”](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) demonstrated compression and write-path savings for Spark logs. [“Modernizing Logging at Uber with CLP (Part II)”](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) covered production ingestion, retention, and viewing. As adoption grew, engineers also needed SQL to select evolving fields, count recurring patterns, and analyze events across files and time ranges for on-call diagnosis, experimentation, and broader analytics. This blog follows that system end to end, from newly arriving data through columnar CLP archives and Presto queries.

Compression alone does not explain the result. Across planning and execution, Presto, Uber's metastore, the connector, and CLP use metadata and encoded structure to avoid unnecessary work.

## When Applications Define Their Own Log Structure

Thousands of applications and platforms at Uber change independently, often with multiple versions running at once. Their log fields, nesting, and value types change with them. One event may contain natural-language text with embedded variables, while another contains nested JSON. The same path may hold an integer, string, or object. The data is **semi-structured** because its fields are discoverable without one predefined schema, and **polymorphic** because one path can hold several value types.

The [second blog in the series](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) explains how Uber preserved existing messages while adding structured operational context. A record can combine natural language, typed fields, and nested objects whose structure evolves independently. The [μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui), presented at OSDI '24 by [YScope](https://yscope.com/), the University of Toronto, and Uber, explains how CLP stores such records.

Requiring one global table schema would turn routine application changes into coordinated migrations. Storing each event as an untyped JSON string avoids those migrations, but every query must parse the string and rediscover its fields and types. CLP instead records structure as events arrive without requiring one schema for the entire dataset.

## Compressed and Queryable as Data Arrives and After Consolidation

A query may span newly emitted events and older consolidated data. CLP keeps both queryable through two compressed storage forms. Host log collectors stream events into appendable [**key-value intermediate representation (KV-IR)**](https://docs.yscope.com/clp/main/dev-docs/design-kv-ir-streams/index.html) files. Later, Uber's aggregation service selects closed KV-IR files for consolidation. A worker streams each selected group into an immutable, columnar CLP archive optimized for compression and search. Consolidation runs asynchronously, so queries continue using KV-IR while the archive is built.

KV-IR carries structure and values in the same compressed stream. When a new path or type appears, its definition is written before the first event that uses it. Every uploaded snapshot is therefore self-contained and searchable. Configurable triggers upload the current KV-IR contents without closing the local stream. As more events arrive, later uploads replace the same object with a longer snapshot. Once Uber's metastore processes the accompanying metadata update, queries can find the new events, typically within 30 seconds to a few minutes of being emitted.

CLP builds archives directly from encoded KV-IR instead of reconstructing and reparsing the original events. As it streams the inputs, CLP merges their path-and-type definitions into an archive-wide map called the **Merged Schema Tree (MST)**. Events with the same paths and value types are appended to an internal columnar group called an **Encoded Record Table (ERT)**. Separate groups avoid filling one wide table with nulls. CLP also stores recurring text patterns, called **logtypes**, separately from typed variable values.

Grouping files with similar structure and text creates more opportunities to deduplicate recurring structure, logtypes, and values across files. Keeping files often queried together in one archive can also reduce archive opens. Archive grouping must balance these benefits against object size, request cost, query latency, and parallelism.

![CLP storage flow from application containers through KV-IR to consolidated archives](figures/clp-storage-flow.svg)

**Figure 1: Host log collectors upload searchable KV-IR snapshots and publish snapshot metadata to Uber's metastore. The metastore selects closed KV-IR files for asynchronous consolidation. Consolidation builds columnar CLP archives and publishes metadata for each new archive.**

After an archive is built, its source KV-IR files remain briefly before deletion, so the same events may exist in both forms. Uber's metastore selects one physical copy for each source file: an archive takes priority over KV-IR, and the newest archive wins if more than one contains it. A query can therefore span both storage forms without returning duplicate events.

## Native Semi-Structured Querying with Presto

Engineers use either Lucene-style log searches or SQL directly. Uber normalizes and validates both inputs before sending SQL to Presto, where existing access controls and tooling apply.

Presto must know the type of every SQL expression while planning a query. A path in CLP, however, may be absent or hold different types across events. An optional Presto table schema can expose common path-and-type pairs, such as `timestamp` and `event.request_id`, as ordinary typed columns without rewriting CLP data.

With or without that schema, the [open-source CLP connector](https://github.com/y-scope/clp-plugin-presto-connector) provides [typed functions](https://docs.yscope.com/presto/connector/clp.html). They can address an exact path, use a wildcard for a variable path segment, or search compatible values across every field. CLP already infers value types from query expressions, and we plan to expose that capability through the Presto connector.

One query can combine these access patterns:

```sql
SELECT
    timestamp, -- optional-schema timestamp column
    event.request_id, -- optional-schema string column
    CLP_GET_STRING('event.request_id'), -- schema-bound path via typed function
    CLP_GET_JSON_STRING() AS event -- complete event without declaring its schema
FROM clp.platform_logs
WHERE timestamp >= current_timestamp - INTERVAL '30' DAY
  AND CLP_GET_BIGINT('event.*.status_code') >= 500 -- wildcard path, integer value
  AND CLP_WILDCARD_STRING_COLUMN() LIKE 'OutOfMemory%' -- string value in any field
ORDER BY timestamp DESC
LIMIT 100;
```

Here, **native** applies at two levels. At the SQL level, users combine ordinary columns with expressions for evolving paths. At the execution level, the connector invokes CLP within each Presto Native worker process and carries field requests and supported filters into the encoded scan. It does not first pass every event through a generic serializer/deserializer (SerDe) to materialize a row, nor reconstruct JSON merely to call `JSON_EXTRACT_SCALAR()`. CLP searches the encoded representation and reconstructs only the requested results.

[YScope's August 2025 blog](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) explains the underlying query rewriting. The preceding **[PrestoDB blog](PRESTO_BLOG_URL)** compares CLP with [Variant](https://parquet.apache.org/docs/file-format/types/variantencoding/) and explains how future Variant operators could simplify exact-path access.

Together, the SQL interface and direct CLP execution provide native semi-structured querying. At Uber's scale, the next challenge is to reduce how many files and encoded values each query touches.

## Exploiting Data Knowledge at Every Layer

The query path reduces work at three points.

**1. Prune files before scheduling.** Uber's metastore compares query filters with per-file metadata such as event-time bounds, application, host, job, and correlation ID. A file that cannot match never becomes a **split**—a file-processing task assigned to one worker—and is never opened. The open-source connector is agnostic to the metadata provider, so each deployment can integrate a provider suited to its catalog and storage system.

**2. Avoid unnecessary older files for latest-N queries.** Most log queries ask for the latest matching events with `ORDER BY timestamp DESC LIMIT N`. Presto's distributed Top-N operator limits the output, but workers normally process every scheduled split. Uber instead uses file time bounds and exact matching-event counts for supported predicates to avoid scheduling older files that cannot enter the latest `N` results. Unlike pruning, a skipped file may contain matches, but those matches are too old to change the result. The decision becomes subtle when file time ranges overlap. The callout below explains the planning algorithm, safe fallback, and optional approximate mode for interested readers.

> **Latest-N file skipping: guaranteed and approximate modes**
>
> In guaranteed mode, candidate retrieval returns pruned files in descending `max_timestamp` order. The metastore accumulates exact matching-event counts. When the count first reaches `N`, it sets a watermark at the minimum `min_timestamp` of the files selected so far. It continues selecting every file whose `max_timestamp` is at or above that watermark, covering overlapping time ranges, and stops at the first file below it. Because later candidates are no newer, they cannot change the latest `N` results.
>
> If any predicate lacks an exact per-file count, guaranteed mode falls back to every file remaining after pruning. Users may instead opt into approximate mode, which uses estimated counts to reduce work but can omit events belonging in the true latest `N`. This planning happens before split creation, so skipped files are never scheduled or opened. Uber runs the optimization in production. It is not yet part of the open-source connector.

**3. Narrow the scan inside each archive.** Before reading encoded events, CLP uses the archive's path-and-type map to find relevant ERTs and columns. Logtype and value dictionaries then rule out text patterns and values that cannot match. CLP advances to the relevant encoded streams and reconstructs only matching results.

**Combined impact.** On average across the measured production query population, time-range filtering alone would require the system to examine approximately 218,660 times as many log events as Uber's full query path. The reduction combines files eliminated by metadata, older files excluded by latest-N planning, and records ruled out by CLP inside selected archives.

These optimizations are most effective for selective queries. During planning, Uber detects broad searches and automatically suggests narrower time or metadata filters. This prompt often leads users to add constraints they already know, substantially reducing the scan. Users can still proceed when they need the complete result.

## Production Results

For workloads now using CLP, approximately 25 PB of raw logs per month becomes 750–850 TB: 29–33× compression and about 97% less storage.[^compression-comparison]

A five-node cluster, with one 48-core Presto Native worker per node, serves Uber's CLP SQL workload directly from object storage. The cluster supports hundreds of users each week and thousands of queries per day. Average response time is approximately two seconds. The 99th-percentile (p99) response time is 26 seconds.

The workers typically use only 5–10% of their CPU capacity. We also rate-limit object-storage reads to reduce parallelism and preserve bandwidth for other workloads. Queries still meet our latency needs, leaving headroom as demand or scope grows.

Only a few engineers maintain the production CLP ingestion and query service alongside their broader responsibilities, including the metastore, consolidation, and five-node Presto cluster.

**[Figure 2 TODO: Plot weekly active users and daily query volume from May through the approved publication cutoff.]**

> **Publication evidence required:** Record the measurement window and calculation method for the average 218,660× reduction in log events examined. Confirm that the user, query-volume, and response-time figures describe the same production query population and approved measurement window. Confirm the MCP traffic share, denominator, and measurement window. Monthly ingest is not per-query scan volume. Confirm publication approval for the scoped maintenance-effort claim.

## What Comes Next

These results reflect the full query path, not compression alone. They also motivate two next steps: smarter data placement and better tools for agents exploring unfamiliar data.

### Smarter Data Placement and Consolidation

**Intelligent tiering.** CLP could be the default queryable tier, with a hot index provisioned only for tables or time ranges whose access frequency, query shape, and latency needs justify it. CLP would retain the longer history and quickly restore selected data to the hot index if demand rises.

**Grouping similar data during consolidation.** CLP's schema and logtype metadata, source metadata, and query history could guide which closed KV-IR files share an archive. Similar inputs increase cross-file deduplication. Placing data that is often queried together in one archive reduces archive opens. Time range, retention, freshness, and archive size would still constrain each group.

### Beyond Guesswork: Helping Agents Find and Understand Logs

More than half of production CLP queries come from AI agents through a [Model Context Protocol (MCP)](https://modelcontextprotocol.io/docs/getting-started/intro) server. Agents use them for root cause analysis, behavior analytics, and exploratory debugging. MCP lets agents run queries, but effective analysis also requires context: which fields exist, which log format patterns remain after filtering, and what wording the data uses.

Without that context, agents guess. To find native connector load failures in the Claude Code data below, for example, an agent might search `message.content[*].content` for `connector AND ("load failed" OR "undefined symbol" OR "not registered")`. The search works only if both the path and terms fit the data. Different wording may return nothing, while broad terms may return more raw events than the agent can use.

CLP already stores much of the missing context. After time, application, or version filters narrow the candidate data, a query service could preload the agent's context with the available fields, the log format patterns that occur, and their counts. The agent could then build a precise follow-up query from what is actually present.

We are exploring three complementary ways to put that information to work. An agent could use any of them independently or combine them in any order:

- **Inspect structure with `SCHEMA(<path>)`.** Return the Merged Schema Tree below a specified path, including observed paths and types.
- **Expose log format patterns with `PATTERN(<path>)`.** Emit the inferred pattern for each value so SQL can project, group, or count it.
- **Search by meaning with `SEMANTIC(<path>, <natural-language query>)`.** Find relevant messages without knowing their exact wording.

> **Example: Which failures recur, and on which branches?**
>
> An agent wants to know which native connector load failures recur and whether they are concentrated on particular source-code branches. [Claude Code](https://docs.anthropic.com/en/docs/claude-code/getting-started) stores tool results in `user` records, with their text under `message.content[*].content`. This query combines `SEMANTIC()` and `PATTERN()` to answer that question without guessing every possible failure phrase.
>
> **Native SQL query**
>
> ```sql
> SELECT
>     CLP_GET_STRING('gitBranch') AS branch, -- source-code branch
>     PATTERN('message.content[*].content') AS failure_pattern, -- inferred format pattern
>     count(*) AS occurrences -- events per branch and pattern
> FROM clp.claude_code_session_transcripts
> WHERE CLP_GET_STRING('type') = 'user' -- Claude Code stores tool results in user records
>   AND SEMANTIC(
>       'message.content[*].content',
>       'tool output reporting a native connector load failure'
>   ) -- search by meaning rather than exact terms
> GROUP BY
>     CLP_GET_STRING('gitBranch'),
>     PATTERN('message.content[*].content')
> ORDER BY occurrences DESC;
> ```
>
> **Illustrative output**
>
> | branch | failure_pattern | occurrences |
> | --- | --- | ---: |
> | `feature/dynamic-connector` | `Failed to load connector library <*>: undefined symbol <*>` | 47 |
> | `feature/dynamic-connector` | `Connector factory <*> was not registered` | 19 |
> | `main` | `Could not open shared object file <*>` | 6 |
>
> The `<*>` placeholders represent changing values. In the first row, 47 events with different library paths and symbol names share one log format pattern. Instead of receiving 72 raw events, the agent sees three recurring branch-and-failure combinations. It can use those patterns in a follow-up query or retrieve examples from the combinations that matter.

These interfaces can operate on CLP's stored schema metadata and deduplicated log format patterns before reconstructing individual events. Semantic matching could evaluate those patterns first, then fetch raw events only as evidence. Working over that reduced representation is what makes this direction practical at petabyte scale. We are refining and evaluating these interfaces.

## Open Source and Collaboration

The core query path is open source. [CLP](https://github.com/y-scope/clp) provides semi-structured compression and search, and the [CLP Presto connector](https://github.com/y-scope/clp-plugin-presto-connector) presents KV-IR and archives through one Presto interface. It supports typed exact and wildcard paths, typed value searches across all fields, complete-event reconstruction, and filter and projection pushdown. Deployments can integrate their own metadata and split providers.

Uber's production deployment adds a metastore that manages billions of file records and performs Top-N file skipping. The query layer above it handles routing, reliability, and operational controls.

YScope engineers led the connector implementation. IBM engineers and the Presto community supported the native connector infrastructure and review. YScope and Uber engineers then extended the connector architecture for Uber's production deployment. The resulting boundary keeps Presto responsible for SQL and distributed execution while CLP searches its compressed representation and reconstructs only the requested results.

## Conclusion

At 25 PB per month, query speed depends on avoiding unnecessary work at every layer. Uber's metastore reduces the files that Presto schedules. Within each selected file, CLP uses encoded structure and metadata to avoid decoding and reconstructing unrelated data. Presto queries updated KV-IR snapshots and columnar archives through one SQL path.

The broader lesson is that a common SQL engine does not require one physical storage model. Presto handles SQL and distributed execution, while CLP preserves and searches application-defined semi-structured data in compressed form. Together, they provide an Uber-wide query service with headroom for more queries and workloads.

Next, we want to put metastore and CLP archive metadata to further use: optimize data placement, help agents generate queries from observed structure and patterns, and supply semantic matches that agents can summarize with supporting evidence.

[^compression-comparison]: The [first blog in the series](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) reported 169× compression for Spark messages alone. Today's 29–33× figure covers a broader set of workloads, with records enriched by operational fields such as hostnames, tags, and trace IDs (UUIDs). Trace IDs are effectively unique and contain little repetition for a compressor to exploit.
