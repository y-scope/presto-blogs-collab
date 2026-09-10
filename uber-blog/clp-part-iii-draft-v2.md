![CLP and Presto connector banner](figures/clp-connector-collaboration-cover-21x9-v13.png)

# Bringing Native SQL to Petabyte-Scale Semi-Structured Logs: Inside Uber’s CLP and Presto Logging Infrastructure (Part III)

## Introduction

CLP entered Uber as a cost-saving measure: our first blog[^clp-part-i] showed it cutting Spark's logging cost by two orders of magnitude. Our second[^clp-part-ii] marked the turn from saving to building, growing CLP into an end-to-end logging platform. Not long after, CLP became the standard practice for semi-structured logs at Uber, adopted for streaming infrastructure such as Kafka and Flink, and ML/AI platforms such as Ray and Michelangelo. These systems now collectively produce roughly 25 PB of logs per month, and the question is no longer what CLP saves but what it can do with that data, today and in what comes next.

At the current scale, engineers rarely know in advance the exact log messages they are looking for. Diagnosis is therefore exploratory: engineers work through a combination of relational operations to narrow the search space. For example, to investigate a sudden spike in failed ride requests surfacing in a Flink job's logs, engineers may `SELECT` the fields that matter, such as `timestamp` and `status_code`, to inspect the raw errors, `GROUP BY` fields such as `city_id` and `app_version` to isolate the failure, and perhaps `JOIN` logs across services to trace the failed requests end to end.

These relational operations map directly to SQL. At Uber, SQL runs on Presto, a distributed SQL query engine that federates a broad range of data sources. Conventionally, semi-structured logs are an awkward fit for native SQL: relational engines expect rigid, pre-defined schemas, whereas semi-structured logs carry dynamic schemas with type polymorphism across events. However, as Uber’s semi-structured logs are compressed in the CLP format, this constraint can be relaxed because CLP infers logs’ schemas during ingestion. With this fundamental schema constraint out of the way, a SQL-native connector for semi-structured logs became possible. All that remained was to build a Presto CLP connector that retrieves data from the CLP format while supporting native SQL operations, as we show in more detail later.

Delivering native SQL compatibility was only the first step. Deploying a production-grade system on petabytes of logs imposed more constraints. When an interactive user query arrives, response time must stay fast without sacrificing real-time log freshness. The convention, when it comes to performance-critical systems with huge amounts of data, was to scale out: provisioning hundreds or even thousands of query workers. We took a different route, and it grew the project from a simple connector into a complete logging infrastructure at Uber. Query performance comes from adding a metastore and a CLP-native engine that aggressively prune unrelated data. Freshness comes from integrating CLP’s streaming Key-Value Intermediate Representation (KV-IR) during log collection, which keeps new events compressed and searchable while data is still arriving. Instead of hundreds or thousands of workers, five were enough to serve production logging at petabyte scale.

The cluster is thus at a maintainable scale, but our maintenance burden simply moved elsewhere. We initially kept the CLP connector inside the Presto repository. However, reconciling upstream changes with Uber-specific customizations on every rebase is costly. Of all its connectors, Uber's Presto team commits active maintenance only on the Hive connector because of this trouble. Therefore, what we needed was a decoupled plugin: an artifact that the Presto team can download and load into Presto through configuration alone.

This is how the Presto CLP connector, led by Uber and YScope, became the first plugin connector in PrestoDB for both the Presto coordinator and native worker,[^coordinator-spi] and a reusable pattern future connectors can build on. As it turned out, this was not a challenge unique to Uber. Engineers at Meta and IBM faced the same problem when trying to adopt emerging data formats without maintaining Presto forks. When we proposed to the PrestoDB community the infrastructure enabling any connector to be installed as a plugin, many people engaged voluntarily and did most of the work. What we contributed was the piece that completed the design end to end, along with the production deployment that validated it at scale. The infrastructure was officially released as part of Presto 0.299.

The rest of this blog presents Uber’s logging infrastructure built upon the Presto CLP connector, which serves hundreds of weekly active users (WAU) and thousands of successful queries per day. The blog is structured as follows: we first explain how we map the evolving, semi-structured schema of logs onto native SQL. Then, we break the infrastructure into ingestion and search flows to show how it reaches petabyte scale. Finally, we show how the design serves as the groundwork for exposing semantics-preserving CLP context to AI agents.

## When Semi-structured Logs Meet Native SQL

### Uber’s Logging Context

With many platforms emitting logs, enforcing a strict centralized schema is impractical. For example, a log field `status_code` may arrive as a number in one log, a string in another, or even wrapped in a nested object such as `request.status_code`. The logging infrastructure must therefore accept data as-is and derive structure automatically.

### Background of CLP and Why It Is the Solution

CLP infers schemas at ingestion time: it incrementally constructs a schema tree to represent all observed structured paths and data types, then groups log events with identical schema into a table. The resulting set of tables, together with their schema metadata, is bundled as a self-contained archive, serving as the basic storage unit in CLP. For natural-language fields such as log messages, CLP also separates recurring static text, stored once as **logtypes**, from changing, typed variables. Both remain linked to the original events. That separation pays off twice later in this blog: search can skip whole columns that cannot match, and an agent could learn what happened, and how often, without reading a single event.

Because tables in archives are derived from raw input on the fly, new fields and type polymorphism extend the schema tree incrementally without any intrusive database table rebuild, such as a schema migration. This addresses the exact pain point of Uber’s evolving log structure. The design also translates directly into search efficiency. Rather than scanning wide, sparse tables filled with empty values, CLP evaluates the query against its schema tree to target only the specific internal columnar tables that match the requested field and data type. As demonstrated in [our OSDI '24 paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui), published jointly by YScope, the University of Toronto, and Uber, CLP achieves **2.47× to 8.09× faster query performance** compared to databases like ClickHouse and PostgreSQL.

### How CLP Integrates with SQL

To allow Presto to query CLP's dynamic columns, we added a handful of `CLP_GET_<TYPE>(<json_path>)` UDFs in the connector, where `<TYPE>` refers to the Presto type of the column and `<json_path>` refers to the column's JSON path. For instance, to reference `request.status_code` as an integer, a user could call `CLP_GET_BIGINT('request.status_code')`. When querying an archive, if CLP does not find the given column name with the given type, it simply contributes no rows from that archive. Inside the connector, the UDF's path and type pinpoint the matching columns across CLP’s many tables, delivering native CLP query speed.

One more UDF deserves mention: `CLP_GET_JSON_STRING()` retrieves the entire log record rather than individual columns. Without it, users would have no practical way to retrieve a whole record, since each record may have a different schema.

The [Presto CLP connector documentation](https://docs.yscope.com/presto/connector/clp.html) lists the full set of supported UDFs.

## Uber's Logging Infrastructure: System Overview

Figure 1 shows the architecture of Uber’s logging infrastructure and the life cycle of a log along its two paths, ingestion and search, and the consolidation step that bridges them.

![Uber's CLP logging infrastructure showing the ingestion and search paths sharing a metastore and object storage](figures/clp-system-overview.svg)

**Figure 1: System overview of Uber’s logging infrastructure**

### Ingestion

CLP archives are built once from a fixed batch of semi-structured data and compress best when that batch is large, since compression depends on repetition across many events. Small batches would mean a flood of tiny, poorly compressed archives, but a single container typically takes far too long to accumulate a batch large enough to compress well, and failure diagnosis cannot wait. **KV-IR**, CLP's append-only, row-based streaming format, resolves this by compressing events incrementally as they arrive and keeping them searchable mid-write. So the pipeline has two tiers: events are compressed into KV-IR per container for freshness, then KV-IR files from many containers are consolidated into one archive.

The ingestion pipeline begins at the edge, where application containers emit semi-structured logs to the local filesystem. A per-host log collector encodes these log files on the fly into KV-IR. Either at a set interval or following a configurable publication policy, described in our second blog,[^clp-part-ii] it uploads the growing file to object storage, replacing the previous snapshot with a longer one, so the latest events are searchable before the file closes.

Meanwhile, the collector streams metadata updates for each upload to Kafka. We capture rich metadata for later optimization: per-file details such as its time range, event count, and whether it is still open, plus context like zone or host ID that is only available at the edge. A polling process consumes them from Kafka and persists them into a relational metastore.

### Consolidation

Consolidation bridges the two tiers. Once a policy-defined batch of closed KV-IR files is registered in the metastore, an asynchronous process merges them into one archive, writes it to object storage, and registers it in the metastore too. Because horizontally scaled containers emit similar logs, a batch merged across many of them is both large and highly repetitive, exactly what CLP compresses best. The archive is also cheap to build. Parsing is modest on any one host but would be expensive to repeat centrally at Uber's volume, so it happens once at the edge, where the fleet forms a massively parallel parsing layer. Because KV-IR already carries each event's structure and encoded values, consolidation never parses anything a second time.

### Search

Engineers and agents issue debugging and analytics queries either through Uber's observability stack, which translates them into SQL and routes them to a Presto cluster, or as SQL sent to Presto directly.

Presto parses the query into a query plan, the sequence of steps it will execute, and our CLP connector steps in to optimize those steps for semi-structured data. It first consults the metastore, matching its metadata against the query's predicates to aggressively prune candidates before any file is opened. Until a consolidated KV-IR file is purged, its events also exist in an archive, so the metastore marks one representation as authoritative and a query never sees duplicates. Any remaining files, along with the predicates that could not be resolved by metadata alone, are then handed to the CLP-native execution engine, which evaluates them directly on the compressed representation without full decompression. Whatever the connector cannot push into CLP, Presto evaluates itself on the rows CLP returns.

## Implementation Details

Next, we explain the implementation details behind the optimizations on both paths.

### Ingestion Optimization

**Near real-time query freshness:** Once the metastore receives a snapshot's metadata update, its new events are immediately queryable. End to end, from an event being written to it being searchable, that typically takes 30 seconds to a few minutes. Publishing at an interval or by policy keeps each collector's update rate low, but across hundreds of thousands of containers it still adds up to thousands of metadata updates per second at peak. Our metastore sustains more than 10,000 `UPSERT` operations per second per table using MySQL's `INSERT ... ON DUPLICATE KEY UPDATE`, leaving ample headroom above the observed peak load.

**Metadata- and policy-guided consolidation:** Closed KV-IR files are considered immutable but remain queryable, so consolidation does not need to begin immediately. A planner waits for a useful batch, then groups related files using file metadata and a pluggable, table-specific policy that can be tuned with domain knowledge about how each workload is queried. A policy might group logs from the same service within a shared time window, keeping similar schemas and recurring text together, or group logs from different layers of a stack that are commonly queried together, reducing the number of archives a query has to open. Workers build the archives asynchronously, outside the ingestion and query paths. This improves compression and search locality without delaying access to the logs.

### Search Optimization

To avoid overwhelming Presto with unneeded data, we introduced three tiers optimizations , applied layer after layer, guided by a single principle: prune the data scanned by the CLP connector and offload work for Presto.

#### Top-level filtering

When a SQL predicate matches metadata registered in the metastore during ingestion (such as timestamp or service field), the connector pushes the predicate down ~~to the `TableScan` node~~. Rather than fetching all candidate IRs and archives from the metastore and defer the filtering to Presto, it appends these pushed-down predicates directly to the metastore lookup query. As a result, the CLP connector receives file candidate listings that are already filtered at the file level.

#### TopN early termination

Investigative queries are predominantly of the form "find the N most recent matching events." Without knowing how many log events an IR file or archive contains before opening it up, the CLP connector must conservatively scan all matching candidates to avoid returning fewer than N results. The key to bypassing this scan again lies in the metastore: during the ingestion, the log collector tracks the exact event count per file and emits  it along with the metadata update. When a query requests the N most recent events, the CLP connector retrieves this count, orders candidate IR files and archives by time, and opens only the minimal subset that is sufficient enough to yield N events. Terminating the moment the count reaches N isn't quite enough on its own though, since an unopened file can still hold a newer match if its time range overlaps the files already counted.

#### CLP-Native vectorized execution

CLP table schema resolution: CLP partitions log events that share the same schema into their own table within an archive. During schema resolution, CLP matches the structure implied by a query against each table's schema and keeps only the tables that could contain a match. For example, if a query filters on `zone`, any table whose schema contains no `zone` field cannot hold a match and is skipped without any scan.

**Dictionary lookup:** This applies specifically to logtypes and their variables, not every column: each is stored once in a dictionary with an ID. Checking that dictionary first means a column with no match is skipped before any row is scanned, and it means a query can match on the pattern, a specific variable, or both, instead of needing to compare the whole string.

**Vectorized Materialization:** Materialization happens last: only the projected columns are read, only at the matching rows, filled directly into Velox vectors since CLP's storage is already columnar.

#### Effectiveness of the optimizations

Across a 30-day production window, these three layers reduced the amount of data that needed to be examined by more than 200,000×. This reflects the combined effect of all three layers, not any one optimization alone.

## Result So Far:

With the optimizations discussed above, using a 5-node Presto cluster, we are servicing hundreds of user per week, handling over thousands of query per day, achieving an average response time of 2 seconds (p99 of 26 seconds).

Furthermore, the architecture’s lightweight design keeps operational overhead low, requiring only a few engineers to maintain the entire infrastructure.

## What Comes Next: Beyond Guesswork for Agents

Agents already issue more than half of CLP queries at Uber for root cause analysis, behavior analytics, and exploratory debugging. The CLP-native filtering layer uses metadata about observed field paths, value types, and recurring message formats to narrow each scan. The same metadata can also help agents understand unfamiliar data before deciding which events to retrieve.

A fixed-schema table has a known list of columns and types. Evolving semi-structured logs do not. Their available fields depend on the selected applications, versions, time range, and other filters. Agents often compensate by guessing field names and combining broad conditions such as `message LIKE '%failed%' OR message LIKE '%exception%' OR message LIKE '%error%'`. Narrow guesses can miss relevant events, while broad searches can bury them in irrelevant results.

CLP can provide this context without a separate index, because the required metadata is part of its compressed representation. Recurring message formats are stored separately from their changing, typed variables, while both remain linked to the exact source events.

Before retrieving raw events, an agent may need to answer three questions about the data remaining after initial filtering: Which fields are present? Which message formats appear, and how often? Which of those formats describe the problem it is trying to solve? We are refining a SQL function for each question.

- `SCHEMA('message')` answers **"What fields are available?"** In datasets such as coding-agent session logs, `message` can be a nested object. The function returns the paths and value types below it. Using the question and, if needed, documentation or source code, the agent can select a relevant field such as `message.content[*].content`.
- `PATTERN('message.content[*].content')` answers **"What happened, and how often?"** by exposing message formats that SQL can group and count, including rare formats that may be buried beneath more common events.
- `SEMANTIC('message.content[*].content', 'native connector load failure')` answers **"Which formats are relevant?"** by matching their semantic meaning instead of requiring exact wording.

These are not required steps. An agent can use any function independently or combine them.

These operations run against CLP's deduplicated metadata rather than raw events, so their cost tracks the diversity of structures and messages rather than the total event count, keeping agent context compact at petabyte scale while staying linked back to exact evidence.

CLP already records each extracted variable's type and position, but not necessarily its meaning. Future enrichment could label a value as a library path, symbol, filename, or job ID. A label applies to one variable position within a recurring format, so enrichment work follows the number of formats and variable positions rather than the number of events. Combined with structured identifiers already present in Uber logs, these labels could improve event grouping and correlation and provide evidence for anomaly detection, summarization, and root-cause-analysis pipelines.

Wide-event enrichment goes further: combining logs with traces, the path one request takes across services, and metrics, aggregated measurements like latency or error rate, into a single, richer record for each request or operation. It also intensifies the schema problem: data sources and enrichment pipelines evolve independently, causing fields, nested structures, and value types to change over time. This is exactly the problem CLP solves by design. CLP can organize every observed structure without imposing a global schema, giving agents a connected view of what happened while retaining links to the exact underlying evidence.

The following example combines metadata discovery, semantic matching, pattern enumeration, and labeled variables. When a user asks an agent to triage potential errors in the connector, the agent first calls `SCHEMA('message')` to find where the relevant text lives, then runs:

```
SELECT
    PATTERN('message.content[*].content') AS failure_pattern,
    COUNT(*) AS occurrences
FROM clp.coding_agent_sessions
WHERE SEMANTIC('message.content[*].content', 'native connector load failure')
GROUP BY PATTERN('message.content[*].content');
```

| enriched failure pattern | occurrences |
| --- | --- |
| Failed to load connector library **`%library_path%`**: undefined symbol **`%symbol%`** | 47 |
| Connector factory **`%factory_name%`** was not registered | 19 |
| Could not open shared object file **`%file_path%`** | 6 |

Instead of reading 72 similar events, the agent sees three formats and their counts. Without this, "Could not open shared object file `%file_path%`," the rarest of the three at just 6 occurrences, would likely stay buried under the other 66 events. Labels such as `%library_path%` and `%symbol%` explain the roles of the changing values. The agent can then retrieve the exact events it needs. A wide event for the affected operation can also provide related transcript messages, tool calls, execution traces, and resource metrics, moving from a recurring pattern to its full execution context without separately discovering every source.

The underlying CLP structures already exist. We are refining and evaluating the interfaces and enrichment needed to expose them to agents across Uber and broader CLP workloads.

[^clp-part-i]: [“Reducing Logging Cost by Two Orders of Magnitude Using CLP”](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) showed how streaming CLP compression reduced storage and SSD writes for Spark logs and pointed to familiar SQL access through Presto as a next step.

[^clp-part-ii]: [“Modernizing Logging at Uber with CLP (Part II)”](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) described production ingestion, retention, and log viewing.

[^coordinator-spi]: Plugin support for the Presto coordinator already existed through Presto's Java Service Provider Interface (SPI). What did not exist was a way to plug a connector into the Presto Native (C++) worker without maintaining a fork, the gap these changes closed.
