# Making Petabyte-Scale Semi-Structured Logs SQL-Native: Inside Uber’s CLP and Presto Infrastructure (Part III)

**Document status:** Working publication draft

At Uber, machine learning, AI, streaming, and data platform workloads using the [Compressed Log Processor (CLP) platform](https://github.com/y-scope/clp) produce approximately 25 PB of raw semi-structured logs each month. Built to compress and search evolving semi-structured data, CLP reduces that volume to 750–850 TB. Yet five 48-core [Presto Native](https://prestodb.io/docs/current/presto-cpp.html) workers—Presto's C++ worker implementation—query it directly from Uber's internal object storage, typically at 5–10% CPU utilization and without staging or caching complete archives. How can such a small query cluster provide SQL access at that scale?

This blog continues Uber's CLP series. The first blog, [“Reducing Logging Cost by Two Orders of Magnitude Using CLP”](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/), demonstrated compression and write-path savings for Spark logs. The second, [“Modernizing Logging at Uber with CLP (Part II)”](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/), covered production ingestion, retention, and viewing. This third blog follows the system end to end: how Uber keeps newly arriving data searchable, consolidates it into columnar CLP archives, and queries both storage forms with Presto.

Compression is only part of the answer. Across planning and execution, Presto, Uber's metastore, the connector, and CLP use different information to avoid unnecessary work when querying fresh KV-IR and archives.

## When Applications Define the Log Structure

Thousands of applications and platforms emit logs at Uber on independent release schedules, often with multiple versions running at once. As a result, log fields, nesting, and value types can change continuously. One event may be natural-language text with embedded variables. Another may be nested JSON. The same path may contain an integer, string, or object. The data is **semi-structured** because its fields are discoverable without one predefined schema, and **polymorphic** because one path can hold several value types.

The [second blog in the series](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) explains how Uber preserved existing messages while adding structured operational context. A record can combine natural language, typed fields, and nested objects whose structure evolves independently. The [μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui) from OSDI '24, by [YScope](https://yscope.com/), the University of Toronto, and Uber, explains how CLP stores such records.

Requiring one global table schema would turn routine application changes into coordinated migrations. Storing each event as an untyped JSON string avoids those migrations, but every query must parse the string and rediscover its fields and types. CLP instead records structure as events arrive without requiring one schema for the entire dataset.

## Compressed and Queryable as Data Arrives and After Consolidation

A query may span newly emitted events and older consolidated data. CLP keeps both queryable through two compressed storage forms. Host log collectors stream events into appendable [**key-value intermediate representation (KV-IR)**](https://docs.yscope.com/clp/main/dev-docs/design-kv-ir-streams/index.html) files. Later, Uber's aggregation service selects closed KV-IR files for consolidation. A worker streams each selected group into an immutable, columnar CLP archive optimized for compression and search. Consolidation runs asynchronously, so queries continue using KV-IR while the archive is built.

Within KV-IR, each new path-and-type definition appears in the same compressed stream as the encoded values, before the first event that uses it. An uploaded snapshot therefore contains everything CLP needs to interpret and search its events. Configurable triggers upload the current KV-IR contents without closing the local stream. As more events arrive, subsequent uploads replace the same object with a longer snapshot. Once Uber's metastore processes a metadata update, queries can find the new events, typically within 30 seconds to a few minutes of being emitted.

Archive construction is efficient because CLP processes encoded KV-IR directly instead of reconstructing and reparsing the original events. While streaming the inputs, CLP merges their path-and-type definitions into an archive-wide map called the **Merged Schema Tree (MST)**. Records with the same paths and value types share an internal columnar group called an **Encoded Record Table (ERT)**. Keeping these groups separate avoids filling one wide table with nulls. CLP also stores recurring text patterns, called **logtypes**, separately from typed variable values.

When consolidation groups files with similar structure and text, CLP has more opportunities to deduplicate structural information, logtypes, and values. Keeping files often queried together in the same archive can also reduce archive opens. Archive grouping must balance these benefits against object size, request cost, query latency, and parallelism.

![CLP storage flow from application containers through KV-IR to consolidated archives](figures/clp-storage-flow.svg)

**Figure 1: Host log collectors upload searchable KV-IR snapshots and publish snapshot metadata to Uber's metastore. The metastore selects closed KV-IR files for asynchronous consolidation. Consolidation builds columnar CLP archives and publishes metadata for each new archive.**

Although KV-IR and archives differ physically, the connector exposes both through one Presto query path.

## Native Semi-Structured Querying with Presto

The second blog showed how engineers view individual compressed logs. Engineers also need to select evolving fields, count recurring patterns, and analyze events across files and time ranges for on-call diagnosis, experimentation, and broader analytics. Some use Lucene-style log searches, while others write SQL directly. Uber normalizes and validates both inputs before sending SQL to Presto, where existing access controls and tooling apply.

Presto must know the type of every SQL expression while planning a query. A path in CLP, however, may be absent or hold different types across events. An optional table schema can bind common path-and-type pairs, such as `timestamp` and `event.request_id`, to ordinary typed columns without rewriting CLP data.

With or without that schema, the current [open-source CLP connector](https://github.com/y-scope/clp-plugin-presto-connector) provides [typed functions](https://docs.yscope.com/presto/connector/clp.html). A function can name an exact path, including one already bound as a column, use a wildcard for a variable path segment, or search compatible values across every field. CLP already supports inferring value types from query expressions, and exposing that capability through the Presto connector is on our to-do list.

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

Here, **native** applies at two levels. At the SQL level, users combine ordinary columns with expressions for evolving paths.

At the execution level, the connector carries field requests and filters directly into CLP. It does not first pass every event through a generic serializer/deserializer (SerDe) to materialize a row. Nor does it reconstruct JSON merely to call `JSON_EXTRACT_SCALAR()`. CLP searches the encoded representation and reconstructs only the requested results.

[YScope's August 2025 blog](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) explains the underlying query rewriting. The preceding **[PrestoDB blog](PRESTO_BLOG_URL)** compares CLP with [Variant](https://parquet.apache.org/docs/file-format/types/variantencoding/) and explains how future Variant operators could simplify exact-path access.

Together, the SQL interface and direct CLP execution provide native semi-structured querying. At Uber's scale, the next challenge is to reduce how many files and encoded values each query touches.

## Exploiting Data Knowledge at Every Layer

Metadata provides that reduction at three points.

**1. Prune files before scheduling.** Uber's metastore compares query filters with per-file metadata such as event-time bounds, application, host, job, and correlation ID. A file that cannot match never becomes a **split**—a file-processing task assigned to one worker—and is never opened. The open-source connector is agnostic to the metadata provider, so each deployment can integrate a provider suited to its catalog and storage system.

**2. Avoid unnecessary older files for latest-N queries.** Most log queries ask for the latest matching events with `ORDER BY timestamp DESC LIMIT N`. Presto's distributed Top-N operator limits the output, but workers normally process every scheduled split. Uber instead uses file time bounds and exact matching-event counts for supported predicates to avoid scheduling older files that cannot enter the latest `N` results. Unlike pruning, a skipped file may contain matches, but those matches are too old to change the result. The decision becomes subtle when file time ranges overlap. The callout below explains the planning algorithm, safe fallback, and optional approximate mode for interested readers.

> **Latest-N file skipping: guaranteed and approximate modes**
>
> In guaranteed mode, the metastore streams pruned candidates by descending `max_timestamp` and accumulates exact matching-event counts. When the count first reaches `N`, it locks a watermark at the minimum `min_timestamp` of the files selected so far. It continues selecting every file whose `max_timestamp` is at or above that watermark, covering overlapping time ranges, and stops at the first file below it. Because later candidates are no newer, they cannot change the latest `N` results.
>
> If any predicate lacks an exact per-file count, guaranteed mode falls back to every file remaining after pruning. Users may instead opt into approximate mode, which uses estimated counts to reduce work but can omit events belonging in the true latest `N`. This planning happens before split creation, so skipped files are never scheduled or opened. Uber runs the optimization in production. It is not yet part of the open-source connector.

**3. Narrow the scan inside each archive.** Before reading encoded events, CLP uses the archive's path-and-type map to find relevant column groups. Logtype and value dictionaries then rule out text patterns and values that cannot match. CLP advances to the relevant encoded columns and reconstructs only matching results.

These optimizations are most effective for selective queries. During planning, Uber detects broad searches and automatically suggests narrower time or metadata filters. That small prompt often leads users to add constraints they already know, substantially reducing the scan. Users can still proceed when they need the complete result.

## Production Results

For workloads now using CLP, approximately 25 PB of raw logs per month becomes 750–850 TB: 29–33× compression and about 97% less storage.[^compression-comparison]

A five-node cluster, with one 48-core Presto Native worker per node, serves Uber's CLP SQL workload directly from object storage. The cluster supports hundreds of users each week and thousands of queries per day. Average response time is approximately two seconds. The 99th-percentile (p99) response time is 26 seconds.

The workers typically use only 5–10% of their CPU capacity. We also rate-limit object-storage reads to reduce parallelism and preserve bandwidth for other workloads. Queries still meet our latency needs, leaving headroom as demand or scope grows.

**[Figure 2 TODO: Plot weekly active users and daily query volume from May through the approved publication cutoff.]**

> **Publication evidence required:** Add one representative timestamp-only versus richer-metadata pruning comparison. Confirm that the user, query-volume, and response-time figures describe the same production query population and approved measurement window. Monthly ingest is not per-query scan volume. Define the operational scope before making a maintenance-effort claim.

## What Comes Next

These production results reflect the full query path, not compression alone. They also motivate two next steps: smarter data placement and better tools for agents exploring unfamiliar data.

### Smarter Data Placement and Consolidation

**Intelligent tiering.** CLP could remain the default queryable tier, with a hot index provisioned only for tables or time ranges whose access frequency, query shape, and latency needs justify it. CLP would retain the longer history and quickly restore selected data to the hot index if demand rises.

**Grouping similar data during consolidation.** CLP's schema and logtype metadata, source metadata, and query history could guide which closed KV-IR files share an archive. Similar inputs increase cross-file deduplication. Placing data that is often queried together in one archive reduces archive opens. Time range, retention, freshness, and archive size would still constrain each group.

### Beyond Guesswork: Putting CLP Metadata to Work for Agents

More than half of current query traffic reaches the production SQL path through a [Model Context Protocol (MCP)](https://modelcontextprotocol.io/docs/getting-started/intro) server. SQL lets agents execute queries, but it does not tell them what to query. With unfamiliar data, agents may not know which paths, types, or patterns exist, or the exact wording to search for. Trying keywords can return nothing or more raw events than the agent can use.

CLP already stores much of the information needed to get started. After filtering by time, application, or version, the query service could place the remaining paths, types, distinct logtypes, and their counts into the agent's context. The agent could then use the observed structure to write a precise query, or search by meaning and summarize matches without reconstructing every event.

Finding relevant events is only the first step. Logs often repeat the same message thousands of times while changing only a timestamp, request ID, path, or other variable. Returning all of them consumes search bandwidth and the agent's limited context while hiding whether the result contains one dominant failure or many different problems.

Three operations could reduce that result in stages. `SEMANTIC(path, description)` finds events by meaning when the agent does not know the exact wording. `PATTERN(path)` then answers, “Which distinct message forms did the search find, and how often?” by grouping messages that differ only in variable values. If the pattern list is still long, `SUMMARY()` could explain the smaller set while preserving counts, representative events, and links to the source evidence. The agent can then decide which pattern merits a deeper query.

> **Example: Which failures recur, and on which branches?**
>
> Suppose an agent wants to know which native connector load failures recur and whether they are concentrated on particular source-code branches. `SEMANTIC()` finds conceptually related [Claude Code](https://docs.anthropic.com/en/docs/claude-code/getting-started) tool output without exact keywords. `PATTERN()` maps the matches to recurring logtypes, and `gitBranch` shows where each one occurred.
>
> ```sql
> SELECT
>     CLP_GET_STRING('gitBranch') AS branch, -- where the failure occurred
>     PATTERN('message.content[*].content') AS failure_pattern, -- recurring message shape
>     count(*) AS occurrences -- events represented by this branch-pattern pair
> FROM clp.claude_code_session_transcripts
> WHERE CLP_GET_STRING('type') = 'user' -- Claude Code stores tool results in user records
>   AND SEMANTIC(
>       'message.content[*].content',
>       'tool output reporting a native connector load failure'
>   ) -- match the concept without guessing its exact wording
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
> Claude Code stores tool results in `user` records, and `message.content` may hold text or typed blocks. The `<*>` placeholders represent variable values. In the first row, 47 events with different library paths and symbol names share one logtype. Instead of reading 72 raw events, the agent sees three recurring branch-and-failure combinations and retrieves individual events only when it needs evidence.

CLP could perform these operations over filtered paths, deduplicated logtypes, dictionary values, and stored counts, deferring event reconstruction until the agent requests examples or source evidence. These directions still need refinement and evaluation.

## Open Source and Collaboration

The core query path is open source. [CLP](https://github.com/y-scope/clp) provides semi-structured compression and search, and the [CLP Presto connector](https://github.com/y-scope/clp-plugin-presto-connector) presents KV-IR and archives through one Presto interface. It supports typed exact and wildcard paths, typed value searches across all fields, complete-event reconstruction, and filter and projection pushdown. Deployments can integrate their own metadata and split providers.

Uber's production deployment adds a metastore that manages billions of file records and performs Top-N file skipping. The query layer above it handles routing, reliability, and operational controls. This production foundation also gives us a practical starting point for the storage and agent improvements described above. We are still developing and evaluating those improvements.

YScope engineers led the connector implementation. IBM engineers and the Presto community supported the native connector infrastructure and review. YScope and Uber engineers then extended the connector architecture for Uber's production deployment. The resulting boundary keeps Presto responsible for SQL and distributed execution while CLP searches its compressed representation and reconstructs only the requested results.

## Conclusion

At 25 PB per month, efficient queries depend on avoiding unnecessary work at every layer. Uber's metastore reduces the files that Presto schedules. Within each selected file, CLP uses its encoded structure and metadata to avoid decoding and reconstructing unrelated data. Presto queries updated KV-IR snapshots and columnar archives through one SQL path.

The broader lesson is that a common SQL engine does not require one physical storage model. Presto handles SQL and distributed execution, while CLP preserves and searches application-defined semi-structured data in compressed form. Together, they provide an Uber-wide query service with substantial capacity for more queries and workloads.

Next, we want to put metastore and CLP archive metadata to further use: optimize data placement, help agents generate queries from observed structure and patterns, and support semantic search and evidence-backed summaries.

[^compression-comparison]: The [first blog in the series](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) reported 169× compression for Spark messages alone. Today's 29–33× figure covers a broader set of workloads, with records enriched by operational fields such as hostnames, tags, and trace IDs (UUIDs). Trace IDs are effectively unique and contain little repetition for a compressor to exploit.
