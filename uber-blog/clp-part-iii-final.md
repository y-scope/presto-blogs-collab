![CLP and Presto connector banner](figures/clp-connector-collaboration-cover-21x9-v13.png)

# Bringing Native SQL to Petabyte-Scale Semi-Structured Logs: Uber's Logging Infrastructure with Presto and CLP (Part III)

## Introduction

Not long after we published our previous blogs[^1][^2] on CLP, what began as a cost-saving log management solution for Spark's logs soon became the standard practice for semi-structured logs at Uber. CLP was soon adopted for streaming infrastructure such as Kafka and Flink, and ML/AI platforms such as Ray and Michelangelo. As of today, these systems collectively produce roughly 25 PB of logs per month.

At the current scale, engineers rarely know in advance the exact log messages they are looking for. Diagnosis is therefore exploratory: engineers work through a combination of relational operations to narrow the search space. For example, to investigate a sudden spike in failed Uber ride requests, engineers may `SELECT` the fields that matter, like `timestamp` and `status_code` to inspect the raw errors, `GROUP BY` fields like `city_id` and `app_version` to isolate the failure, and perhaps `JOIN` logs across services to trace the failed requests end to end.

These relational operations map directly to SQL. At Uber, SQL runs on Presto, a distributed SQL query engine that federates a broad range of data sources. Conventionally, semi-structured logs are an awkward fit for native SQL: relational engines expect rigid, pre-defined schemas, whereas semi-structured logs carry dynamic schemas with type polymorphism across events. However, Uber's semi-structured logs are now compressed in the CLP format: it discovers structure during ingestion and encodes it into a compressed form that remains directly searchable. Such adoption not only clears the schema constraint out of the way, but more importantly provides rich metadata and performant search over compressed semi-structured logs, which made a SQL-native connector for semi-structured logs possible; all that remained was to build a Presto CLP connector that retrieves data from the CLP format while supporting native SQL operations, as we show in more detail later.

Delivering native SQL compatibility with CLP format is only the first step; more constraints were imposed when deploying a production grade system operating on petabytes scales. When an interactive user query arrives, response time must be fast without sacrificing real-time log freshness. The convention, when it comes to performance critical system with huge amounts of data, was to scale out: provisioning hundreds or even thousands of query workers. However, there's more to be leveraged: and what we have leveraged grew the project from a simple Presto connector into a complete logging infrastructure at Uber. Query performance comes from a metastore and a CLP-native engine, which together prune the data footprint that must be scanned. Freshness comes from integrating CLP's streaming Key-Value Intermediate Representation (KV-IR) into the log collector's ingestion path, which keeps new events compressed and searchable while data is still arriving. With these optimizations, instead of hundreds or thousands of worker nodes, five are all you need to serve production logging at petabyte scale.

The cluster is thus at a maintainable scale, but our maintenance burden simply moved elsewhere. We initially kept the CLP connector inside the Presto repository. However, reconciling upstream changes with Uber-specific customizations on every rebase is costly; of all its connectors, the Uber Presto team only commits active maintenance on the Hive connector because of this trouble. Therefore, what we needed was a decoupled plugin: an artifact that the Uber Presto team can download and load into Presto through configuration alone.

This led to the [Presto CLP connector](https://github.com/y-scope/clp-plugin-presto-connector), the first plugin connector[^3] in PrestoDB, led by Uber and YScope and released as an open source project. As it turned out, the problem is not unique to Uber. Engineers at Meta and IBM faced the exact same problem when adopting emerging data formats without maintaining their own Presto forks. When we proposed the plugin infrastructure required for the Presto CLP connector to the PrestoDB community, many contributors, particularly those engineers who faced the very same problem at Meta and IBM, volunteered to help us complete the work. What we built for one connector now works for any connector: implement the interface, and it can be plugged into Presto through configuration alone. This infrastructure was officially released as part of Presto 0.299.

The rest of this blog presents Uber's logging infrastructure built upon the Presto CLP connector, which serves hundreds of weekly active users (WAUs) with thousands of queries per day. The blog is structured as follows: we first explain how we map the evolving, semi-structured schema of logs onto native SQL. Then, we break the infrastructure into ingestion and search flows to show how it scales to petabyte order. Finally, we describe how the design serves as the groundwork for driving semantics-preserving CLP context to AI agents for more capability.

## When Semi-structured Logs Meet Native SQL

### How CLP Stores Semi-Structured Logs

With many platforms emitting logs, a field such as `status_code` may arrive as a number in one log, a string in another, or wrapped in a nested object such as `request.status_code`, so the infrastructure must accept data as-is and derive structure automatically.

Unlike conventional columnar formats such as Parquet or ORC, which are designed primarily for data with well-defined schemas, CLP's storage format is purpose-built for data with dynamic, continuously evolving schemas and nested structures. CLP automatically discovers structure during ingestion and applies specialized encoding and compression techniques to achieve significantly higher compression, while enabling efficient search and analytics directly on the compressed data.

As demonstrated in [our OSDI '24 paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui), published jointly by YScope, the University of Toronto, and Uber, CLP achieves an average compression ratio of 68.1:1 — at least 2.6× higher than existing semi-structured databases, alongside with up to **8.09× faster query performance** compared to databases like ClickHouse and PostgreSQL, on datasets drawn largely from Uber's production logs.

CLP offers more than high compression and fast search with dynamic ingestion, it understands what logs are made of and respects the semantic they bring. For natural-language fields such as log messages, CLP separates recurring static text from changing, typed variables. It stores each recurring message pattern once as a **logtype**, while keeping both parts linked to the original events. Managing logtypes is also cheap: they don't grow with the data size, and they are typically orders of magnitude smaller than the raw log data. That separation pays off twice later in this blog: by only searching the logtypes, search can skip whole columns that cannot match, and an agent could learn what happened, and how often, without reading a single event.

### How CLP Integrates with SQL

SQL engines normally learn a table's column names and types up front, from a `CREATE TABLE` statement. With the CLP connector, there is no such step. To allow Presto to query the columns from CLP, we added a family of `CLP_GET_<TYPE>(<json-path>)` user-defined functions (UDFs) to the connector, where `<TYPE>` refers to the Presto type of the column and `<json_path>` refers to the column's JSON path. For instance, `CLP_GET_BIGINT('request.status_code')` references `request.status_code` as an integer. The declared type is not a cast: CLP already recognized this field as an integer during compression and encoded it in binary, so the connector uses the type during schema resolution to pinpoint the tables holding integer `status_code` values, and a filter like `>= 500` runs as a native integer comparison.

There are more powerful UDFs that CLP can expose to leverage CLP's native; for example: `CLP_GET_JSON_STRING`, retrieves the entire event, which is otherwise impractical when every event may have a different schema. The [Presto CLP connector documentation](https://docs.yscope.com/presto/connector/clp.html) lists the full set of supported UDFs.

## The Uber Logging Infrastructure - System Overview

Figure 1 shows the architecture of Uber's logging infrastructure and the life cycle of a log along its two paths, ingestion and search.

![Figure 1: Architecture of Uber's logging infrastructure](figures/clp-system-overview.svg)

### Ingestion

The ingestion pipeline employs a two-tier storage model. Because CLP archives are immutable, the prerequisite of building an archive is waiting for a log stream to be closed. For infrequently written logs, this can introduce delays up to hours, which is unacceptable for time-sensitive tasks like failure diagnosis. More importantly, logs are distributed on edge hosts; the volume generated by a single host is rarely large enough to achieve CLP's optimal compression ratio. To solve this, CLP has an intermediate representation called **KV-IR**, an append-only, row-based streaming format that compresses logs incrementally as they arrive, making them immediately searchable mid-write. Our ingestion pipeline adopts a hybrid approach: raw logs are compressed into KV-IR for freshness, then consolidated into archives for better compression ratio and search performance.

The ingestion pipeline begins at the edge, where application containers emit logs to the local filesystem. A per-host log collector encodes these log files on the fly into the append-only KV-IR format, periodically flushing encoded IR snapshots to object storage. Meanwhile, it streams metadata updates for the flushed IR files to Kafka; we capture rich metadata for our optimization purposes, examples include per-file metadata such as, state of the IR file, bytes written so far, and context like zone or host ID which are only accessible at the edge. At the consumer side of the Kafka stream, a polling process reads these metadata updates and persists them into a relational metastore. Finally, once enough closed IR files are registered in the metastore via Kafka updates, an asynchronous consolidation process merges them into archives.

### Search

The search pipeline proceeds as follows. Engineers and agents issue debugging and analytics queries through Uber's observability stack, which translates them into SQL and routes them to a Presto cluster. Presto's distributed SQL engine parses the query and produces a query plan. Here, the CLP connector optimizes this plan for performance: it first consults the metastore, whose metadata is matched against the query's predicates to aggressively prune files before any IR file or archive is opened. Any remaining files, along with the predicates that could not be resolved by metadata alone, are then handed to the CLP-native execution engine, which evaluates them directly on the compressed representation without full decompression.

## Implementation Details

The optimizations below follow the two paths from the system overview: ingestion first, then search.

### Ingestion Optimization

#### Near Real-Time Query Freshness

Once the metastore receives a metadata update for an IR snapshot from the log collector, log events from that snapshot become queryable. Counting from the moment an application emits a log, this typically takes 30 seconds to a few minutes, a window governed by the snapshot flush period. Across hundreds of thousands of containers, this adds up to thousands of metadata updates per second of write traffic to the metastore. Our metastore sustains more than 10,000 `UPSERT` operations per second per table using MySQL's `INSERT ... ON DUPLICATE KEY UPDATE`, well above the bursts observed at peak hours.

#### Policy-Guided Consolidation

Because closed KV-IR files remain queryable, consolidation need not begin immediately. A planner waits until a useful batch accumulates, then groups related files using file metadata, with a table-specific policy that can be tuned with domain knowledge about how each workload is queried. A policy might group logs from the same service within a shared time window, keeping similar schemas and recurring text together, or group logs from different layers of a stack that are commonly queried together, reducing the number of archives a query has to open and improving search locality.

### Search Optimization

Recall that Presto turns a query into a plan of steps executed top to bottom. For a typical query, those steps are split generation, table scan, filter, aggregation, and projection. Split generation decides which files to read and packages them as splits, the units of work for each worker to execute. Table scan reads those splits, and the remaining steps operate on the rows that we materialize back from the CLP format. In the planning phase, we also decide which predicates to push down and which fields the CLP must read for the filters, aggregations, joins, and projections still to come.

Our CLP connector intervenes in those first two steps. During split generation it applies two tiers of optimization, top-level filtering and TopN early termination, that use metastore metadata to avoid opening files at all. During table scan it applies a third, CLP-native vectorized execution, to read as little as possible from each file it does open. Figure 2 shows where each sits in the plan.

![Figure 2: CLP connector optimizations in the Presto query plan](figures/clp-search-optimization.svg)

**Top-Level Filtering**

The metastore serves as the top-level index. When a SQL predicate matches metadata registered during ingestion, such as a timestamp or service field, the connector resolves that predicate against the metastore rather than fetching every possible KV-IR and archive and leaving the filtering to Presto. The list of KV-IR and archives the CLP connector receives is therefore already pruned at file granularity.

**TopN Early Termination**

Investigative queries are predominantly of the form "find the N most recent matching events." Without knowing how many log events a KV-IR or archive has before opening it, the connector would have to scan all of KV-IRs and archives matching the top-level filtering to be sure of N results. The key, again, lies in the metastore, which holds each KV-IR and archive's event count from ingestion. When a query asks for the N most recent events, the connector retrieves these counts, orders the splits by time, and opens only the newest files needed to reach N. Here, a caveat is that reaching N is not sufficient for a correct top-N, because a KV-IR or archive spans a time interval rather than a specific point in time: a file that ranks older by interval may still hold events recent enough to belong in the top N. Therefore, our connector may need to open more files than the counts alone require, for functional correctness.

**CLP-Native Vectorized Execution**

Once a split is open, CLP itself narrows the work in three steps.

**CLP table schema resolution:** During schema resolution, CLP matches the structure implied by a query against each table's schema and keeps only the tables that could contain a match. For example, if a query filters on `zone`, any table whose schema contains no `zone` field cannot hold a match and is skipped without any scan.

**Dictionary lookup:** This applies specifically to logtypes and their variables, not every column: each is stored once in a dictionary with an ID. Checking that dictionary first means a column with no match is skipped before any row is scanned, and a query can match on the pattern, a specific variable, or both, instead of comparing whole strings.

**Vectorized materialization:** Materialization happens last. At the matching rows only, the connector reads whatever columns downstream steps still need: the final projection, the inputs to any aggregation or join, and the columns behind filters that could not be pushed down. As CLP's layout is already columnar, the connector fills the value directly into Velox vectors - the in-memory columnar format that Presto's native workers operate on. Accelerated by the Velox engine, materialization of CLP data is both native and columnar.

## Result So Far

Stored in CLP, the roughly 25 PB of logs produced each month occupy 750–850 TB in object storage, 29:1 to 33:1 compression ratio or roughly 97% less storage footprint.[^4]

Across a 30-day production window, the three search optimization tiers cut the data examined by more than 200,000×, the combined effect of all three rather than any one alone.[^5] That reduction is why a five-node[^6] Presto cluster, reading directly from object storage with no cache, serves hundreds of WAUs and completes thousands of successful queries per day, with a mean response time of roughly two seconds and 99% completing within 26 seconds. These are not trivial queries: they can span 30 days of data, hundreds of gigabytes that a naive engine would scan in full. They are, however, filtered — by time range, by field predicates, and by text match — and that filtering is what our optimization is targeted at.

Workers typically use only 5–10% of their CPU and memory because the three tiers eliminate most work before it reaches them. We preserve the remaining headroom deliberately, capping the cluster's object-storage read rate and parallelism to avoid disrupting other workloads while still meeting latency needs. The lightweight design also keeps operational overhead low, requiring only a few engineers to maintain the entire infrastructure.

## What Comes Next: Beyond Guesswork for Agents

Adding SQL syntax was not the hard part. The hard part was keeping petabytes of evolving logs fresh, compact, and queryable while preventing most of that data from ever becoming query work. CLP's own structure and the metastore narrow each query to the data that can matter, KV-IR keeps that data fresh, consolidation keeps it compact, and the plugin path keeps the connector maintainable. The same structure that makes queries cheap is also what comes next.

Agents already issue more than half of CLP queries at Uber, and they start at a disadvantage. A fixed-schema table has a known list of columns. Evolving logs do not, so agents guess field names and write broad searches such as `message LIKE '%failed%' OR message LIKE '%error%'`. Narrow searches miss relevant events, while broad searches bury them among irrelevant results. What an agent needs is an inventory of the slice in front of it: which fields exist, which message patterns occur, and how often. CLP already keeps that inventory as compact metadata: the schema tree of field paths and types, and the logtype and variable dictionaries the third tier uses to narrow each scan, each stored once and linked to its source events.

We are developing three SQL functions that let agents read that inventory directly, merged across every archive and KV-IR file the query's filters select: `SCHEMA()` answers what fields are available by returning the paths and value types below a field, `PATTERN()` answers what happened and how often by exposing message patterns that SQL can group and count, and `SEMANTIC()` answers which patterns are relevant by matching meaning rather than exact wording. Because they read each selected file's compact metadata rather than scanning its events, their cost follows structural and message diversity, not event volume. Suppose an engineer asks an agent why native connector loads keep failing. The agent first calls `SCHEMA('message')` to learn where the message text lives, then uses `SEMANTIC()` to keep only messages that mean "native connector load failure," whatever their exact wording, and `PATTERN()` to group what remains by recurring message pattern:

```sql
SELECT
    -- recurring pattern, variables shown as placeholders
    PATTERN('message.content[*].content') AS failure_pattern,
    COUNT(*) AS occurrences
FROM clp.coding_agent_sessions
-- match by meaning of value on specific field, not exact words
WHERE SEMANTIC('message.content[*].content', 'native connector load failure')
GROUP BY 1; -- one row per pattern
```

| failure pattern | occurrences |
| --- | --- |
| Failed to load connector library **`%library_path%`**: undefined symbol **`%symbol%`** | 47 |
| Connector factory **`%factory_name%`** was not registered | 19 |
| Could not open shared object file **`%file_path%`** | 6 |

A text search for `fail` or `error` would miss the third pattern entirely, since its message contains neither word, and a search broad enough to catch it would bury it. Grouped by pattern, the rare failure is a visible row, and the agent can retrieve exactly those six events.

The placeholders in that table, `%library_path%` rather than a generic type such as `int`, `float`, or `str`, come from CLP+, an experimental extension of CLP that tags each variable's role at ingestion. Because a role is assigned once per pattern rather than once per event, tagging stays cheap. And once a value is known to be, say, a job ID, every event that mentions that job can be linked together, whatever pattern each came from.

Tying events together is also where the observability industry is heading, under a name of its own. A **wide event**, as several platforms now call it, is one record per request or operation carrying the fields of its logs, its trace, and its metrics together. We already have the pieces: [Jaeger](https://www.jaegertracing.io/) records a request's path across services as spans, [M3](https://m3db.io/) and a growing [OpenTelemetry](https://opentelemetry.io/) footprint collect its metrics, and the trace ID ties them together, since spans and logs carry it and metrics can too when configured. A wide event would join them once, at ingestion, so an agent reads one record instead of visiting three systems. Not every log has a trace or metric, and not every trace or metric has a log, so one record may carry three signals and the next only one. Records whose shape changes from one event to the next are what CLP was built to absorb.

CLP entered Uber to cut a bill. What it stores now is the shape of everything Uber logs, and that shape, not a new index, is what would let an agent see what happened without reading all of it.

---

[^1]: ["Reducing Logging Cost by Two Orders of Magnitude Using CLP"](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) showed how streaming CLP compression reduced storage and SSD writes for Spark logs and pointed to familiar SQL access through Presto as a next step.

[^2]: ["Modernizing Logging at Uber with CLP (Part II)"](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) described production ingestion, retention, and log viewing.

[^3]: Plugin support for the Presto coordinator already existed through Presto's Java Service Provider Interface (SPI). What did not exist was a way to plug a connector into the Presto Native (C++) worker without maintaining a fork; our work closed this gap and made the first complete plugin connector for both the Presto coordinator and the worker in PrestoDB.

[^4]: Our first blog reported 169× compression for Spark messages alone. The current aggregate figure covers a broader set of workloads whose records include operational fields such as hostnames, tags, and high-cardinality trace IDs (UUIDs). High-cardinality trace IDs are nearly unique and give the compressor little repetition to exploit. Even so, the aggregate compression ratio still works out to roughly 97% less storage.

[^5]: The figure divides the aggregate number of events covered by successful queries' time ranges during the 30-day window by the aggregate number of events those queries examined after top-level filtering, exact TopN early termination where applicable, and CLP-native filtering.

[^6]: Each worker node uses an over-provisioned 48-core, 384 GB of RAM, not because the workload requires it today but because it is a widely available SKU already used by the Uber Presto team, which keeps our deployment aligned with established operational patterns and avoids fragmentation, and because selecting a standard shape that is routinely approved avoids the slow quota process associated with more customized requests, reducing administrative friction, accelerating deployment, and giving us headroom for future data growth without repeated coordination overhead.
