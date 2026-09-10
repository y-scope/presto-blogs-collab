![CLP and Presto connector banner](figures/clp-connector-collaboration-cover-21x9-v13.png)

# Bringing Native SQL to Petabyte-Scale Semi-Structured Logs: Inside Uber’s CLP and Presto Logging Infrastructure (Part III)

## Introduction

---

CLP entered Uber as a cost-saving measure: our first blog[^clp-part-i] showed it cutting Spark's logging cost by two orders of magnitude. Our second[^clp-part-ii] marked the turn from saving to building, growing CLP into an end-to-end logging platform. Not long after, CLP became the standard practice for semi-structured logs at Uber, adopted for streaming infrastructure such as Kafka and Flink, and ML/AI platforms such as Ray and Michelangelo. These systems now collectively produce roughly 25 PB of logs per month, and the question is no longer what CLP saves but what it can do with that data, today and in what comes next.

At the current scale, engineers rarely know in advance the exact log messages they are looking for. Debugging usually starts with a combination of relational operations that track an error’s lifecycle. For example, to investigate a sudden spike in failed ride requests surfacing in a Flink job's logs, engineers may `SELECT` the fields that matter, such as `timestamp` and `status_code`, to inspect the raw errors, `GROUP BY` fields such as `city_id` and `app_version` to isolate the failure, and perhaps `JOIN` logs across services to trace the failed requests end to end.

These relational operations map directly to SQL. At Uber, SQL queries are executed through Presto, a distributed SQL query engine for a broad range of data sources. Conventionally, semi-structured logs are an awkward fit for native SQL: relational engines expect rigid, pre-defined schemas, whereas semi-structured logs carry dynamic schemas with type polymorphism across events. Once logs are compressed in the CLP format, however, that constraint relaxes, because CLP derives each log's schema on the fly. All that remained was a dedicated Presto connector to map SQL semantics onto CLP's native search.

Delivering native SQL compatibility was only the first step. Deploying a production-grade system on petabytes of logs imposed more constraints. When an interactive user query arrives, response time must stay fast without sacrificing real-time log freshness. The convention, when it comes to performance-critical systems with huge amounts of data, was to scale out: provisioning hundreds or even thousands of query workers. We took a different route, and it grew the project from a simple connector into a complete logging infrastructure at Uber. Query performance comes from adding a metastore and a CLP-native engine that aggressively prune unrelated data. Freshness comes from integrating CLP’s streaming Key-Value Intermediate Representation (KV-IR) during log collection, which keeps new events compressed and searchable while data is still arriving. Instead of hundreds or thousands of workers, five were enough to serve production logging at petabyte scale.

Performance and freshness were not the only hurdles. Integrating the CLP connector into the Presto codebase posed long-term maintenance concerns. Reconciling upstream changes with Uber-specific customization on every rebase is costly, so much so that Uber's Presto team actively maintains only the Hive connector. What we needed was a decoupled plugin: the Presto team downloads the CLP connector artifact and loads it into Presto through simple configuration.

This is how the Presto CLP connector, led by Uber and YScope, became the first end-to-end plugin connector for Presto's native execution engine,[^coordinator-spi] and a reusable pattern future connectors can build on. As it turned out, this was not a challenge unique to Uber. Engineers at Meta and IBM faced the same problem when trying to adopt emerging data formats without maintaining Presto forks. When we proposed to the PrestoDB community the infrastructure enabling any connector to be installed as a plugin, many people engaged voluntarily and did most of the work. What we contributed was the piece that completed the design end to end, along with the production deployment that validated it at scale. The infrastructure was officially released as part of Presto 0.299.

In the rest of this blog, we present Uber’s logging infrastructure that builds upon the Presto CLP connector, serving hundreds of weekly active users (WAU) and thousands of successful queries per day. The blog is structured as follows: we first explain how we map the evolving, semi-structured schema of logs onto native SQL. Then, we break down Uber’s logging infrastructure into ingestion and search flows to show how it reaches petabyte scale. Finally, we show how the design serves as the groundwork for exposing semantics-preserving CLP context to AI agents.

## When Semi-structured Logs Meet Native SQL

---

#### Uber’s Logging Context

Uber generates petabytes of semi-structured log data every day. With various platforms emitting logs, enforcing a strict centralized schema is impractical. For example, a log field `status_code` may arrive as a number in one log, a string in another, or even wrapped in a nested object such as `request.status_code`.Therefore, the logging infrastructure must accept data as-is and derive structure automatically.

#### Background of CLP & Why It Is the Solution

Unlike traditional databases that enforce a strict schema-on-write paradigm, CLP infers schemas dynamically at ingestion time. It incrementally constructs a schema tree to represent all observed structured paths and data types, then grouping the log events with identical schema into a table. Finally, the resulting set of tables, together with their schema metadata, are bundled as a self-contained archive, serving as the basic storage unit in CLP. For natural-language fields such as log messages, CLP also separates recurring static text, stored as logtypes, from changing, typed variables. Both remain linked to the original events.For natural-language fields such as log messages, CLP also separates recurring static text, stored as logtypes, from changing, typed variables. Both remain linked to the original events.

Because tables in archives are derived from raw input on the fly, new fields and type polymorphism  imply extend the schema tree incrementally without requiring any intrusive database reconstruction, such as schema migration. This solves the exact pain point of Uber’s evolving log structure. The design also translates directly into search efficiency. Rather than scanning wide, sparse tables filled with empty values, CLP evaluates the query against its schema tree to target only the specific internal columnar tables that match the requested field and data type. As demonstrated in our OSDI '24 paper, CLP achieves **2.47× to 8.09× faster query performance** compared to databases like ClickHouse and PostgreSQL.

#### How CLP Integrates with SQL

To allow Presto to query the dynamic columns from CLP, we added a handful of `CLP_GET_<TYPE>(<json-path>)` UDFs in the connector, where `<TYPE>` refers to the Presto type of the column and `<json_path>` refers to the column's JSON path. For instance, to reference the `request.status_code`, a user could use `CLP_GET_STRING('request.service')`. When querying an archive, if CLP does not find the given column name with the given type, it will simply return an empty result set. Inside the connector, it maps the column in the UDF to pinpoint the matching columns across CLP’s many tables to deliver native CLP query speed.

There’s more powerful UDF that CLP can expose to leverage CLP’s native; for example: `CLP_GET_JSON_STRING`, is useful for retrieving the entire log record rather than individual columns; without this UDF, users would have no practical way of retrieving an entire record since, given their dynamic schemas, each record may have a different set of schemas.

For the full list of supported UDFs, please see the Presto CLP Connector documentation.

## The Uber Logging Infrastructure - System Overview:

---

The architecture of the Uber’s logging infrastructure is shown below, it shows the life cycle of the logs in two path: ingestion and search.

#### Ingestion:

The ingestion pipeline employs a two-tier storage model. Because CLP archives are immutable, the pre-requisite of building an archive is waiting for a log stream to be closed . For infrequently written logs, this can introduce delays up to hours, which is unacceptable for time-sensitive tasks like failure diagnosis. To solve this, CLP has an intermediate representation called **KV-IR**, an append-only, row-based streaming format that compresses logs incrementally as they arrive, making them immediately searchable mid-write. Our ingestion pipeline adopts a hybrid approach: raw logs are compressed into KV-IR for freshness, then consolidated into archives to better compression ratio.

The ingestion pipeline begins at the edge, where application containers emit semi-structured logs to the local filesystem. A per-host log collector encodes these log files on the fly into the append-only KV-IR format, periodically flushing encoded IR snapshots to object storage. Meanwhile, it streams metadata updates for the flushed IR files to Kafka; we capture much metadata for our optimization sake, examples include per-file metadata such as, state of the IR file, bytes written so far, and context like zone or host ID which are only accessible at the edge. At the consumer side of the Kafka stream, a polling process reads these metadata update and persists them into a relational metastore. Finally, once enough closed IR files are registered in the metastore via Kafka updates, an asynchronous consolidation process merges them into archives.

#### Search:

The search pipeline proceeds as follows.  Engineers issue debugging and analytics queries through Uber's observability stack, which translates them into SQL and routes them to a Presto cluster.  Presto's distributed SQL engine parses the query and produces a query plan. Here, our CLP connector optimizes this planning for logging. It first consults the metastore, whose metadata is matched against the query's predicates to aggressively prune files before any IR file or archive is opened. Any remaining files, along with the predicates that could not be resolved by metadata alone, is then handed to the CLP-native execution engine, which evaluates them directly on the compressed representation without full decompression.

### Implementation Details

Next, we explain some implementation details for the optimization we have done.

#### Ingestion Optimization

**Near real-time query freshness**: Host log collectors upload updated snapshots of growing KV-IR files along with the corresponding metadata. As soon as the metastore applies an update, the new events become available to queries. During peak periods, collectors across Uber's fleet generate thousands of metadata updates per second. Our internal metastore sustains more than 10,000 `UPSERT` operations per second per table using MySQL's `INSERT ... ON DUPLICATE KEY UPDATE`, leaving ample headroom above the observed peak load.

**Metadata- and policy-guided consolidation:** Closed KV-IR files are considered immutable but remain queryable, so consolidation does not need to begin immediately. A planner waits for a useful batch, then uses file metadata and table-specific policies to group related files, such as logs from the same service within a shared time window. Grouping files with similar schemas and recurring text gives CLP more repetition to compress, while grouping data commonly queried together reduces the number of archives opened. Workers build the archives asynchronously, outside the ingestion and query paths. This improves compression and search locality without delaying access to the logs.

Search Optimization

To avoid overwhelming Presto with unneeded data, we introduced three tiers optimizations , applied layer after layer, guided by a single principle: prune the data scanned by the CLP connector and offload work for Presto.

Top-level filtering
When a SQL predicate matches metadata registered in the metastore during ingestion (such as timestamp or service field), the connector pushes the predicate down ~~to the `TableScan` node~~. Rather than fetching all candidate IRs and archives from the metastore and defer the filtering to Presto, it appends these pushed-down predicates directly to the metastore lookup query. As a result, the CLP connector receives file candidate listings that are already filtered at the file level.

**TopN early termination**
Investigative queries are predominantly of the form "find the N most recent matching events." Without knowing how many log events an IR file or archive contains before opening it up, the CLP connector must conservatively scan all matching candidates to avoid returning fewer than N results. The key to bypassing this scan again lies in the metastore: during the ingestion, the log collector tracks the exact event count per file and emits  it along with the metadata update. When a query requests the N most recent events, the CLP connector retrieves this count, orders candidate IR files and archives by time, and opens only the minimal subset that is sufficient enough to yield N events. Terminating the moment the count reaches N isn't quite enough on its own though, since an unopened file can still hold a newer match if its time range overlaps the files already counted.

**CLP-Native vectorized execution**

CLP table schema resolution: CLP partitions log events that share the same schema into their own table within an archive. During schema resolution, CLP matches the structure implied by a query against each table's schema and keeps only the tables that could contain a match. For example, if a query filters on `zone`, any table whose schema contains no `zone` field cannot hold a match and is skipped without any scan.

**Dictionary lookup:** This applies specifically to logtypes and their variables, not every column: as our previous Uber blog explained, each is stored once in a dictionary with an ID. Checking that dictionary first means a column with no match is skipped before any row is scanned, and it means a query can match on the pattern, a specific variable, or both, instead of needing to compare the whole string.

**Vectorized Materialization:** Materialization happens last: only the projected columns are read, only at the matching rows, filled directly into Velox vectors since CLP's storage is already columnar.

**Effectiveness of the optimizations**

Across a 30-day production window, these three layers reduced the amount of data that needed to be examined by more than 200,000×. This reflects the combined effect of all three layers, not any one optimization alone.

## Result So Far:

---

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
