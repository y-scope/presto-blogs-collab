# CLP Part III: Revised Story Compared with the Previous Draft

## Purpose

This document records why the article was restructured. It compares the previous query-focused draft with the revised three-part story and identifies the facts that still require verification.

## Bottom line

The revised plan is broader than the previous article. The previous draft was primarily a **query-serving architecture story**: how Uber makes a large, evolving CLP corpus available through Presto while avoiding enough work to serve it with five lightly utilized workers. The revised draft is an **end-to-end production-system story** with three co-equal takeaways:

1. Convert unstructured, polymorphic logs into columnar SQL through the Presto CLP connector.
2. Ingest logs from many independent producers with a simple, scalable pipeline.
3. Search the resulting 25 PB/month workload with a small Presto cluster.

This is not just a reordering: it adds a new architectural pillar, new production claims, and a new figure.

## What changes

| Dimension | Previous draft | Revised story | Editorial consequence |
|---|---|---|---|
| Central question | How can five Presto Native workers provide SQL access to roughly 25 PB/month of evolving logs? | How does the complete CLP system ingest, represent, and search that workload? | Expand the article from query architecture to an end-to-end system case study. |
| Narrative shape | Production result → schema problem → query routing → connector → work avoidance → impact | Adoption history → SQL need → connector → ingestion → search → evaluation | Ingestion becomes a first-class section instead of background. |
| Ingestion | KV-IR gives recent access; asynchronous aggregation creates immutable archives. | Per-host collectors monitor and enrich container logs, incrementally compress them into appendable KV-IR, publish data periodically, and send metadata updates through Kafka. | Clarifies that compression begins on the host; aggregation later creates a denser archive representation. |
| Connector | Explains SQL APIs, native execution, pushdown, and split-local structure. | Emphasizes why the connector is more than a SQL compatibility layer. | Keep the mechanism, but connect every detail to preventing parsing, materialization, or unnecessary reads. |
| Search | Metadata pruning, Top-N early termination, encoded execution, and direct object-storage reads explain the small cluster. | Proposed user volume, AI/MCP traffic, queries per day, latency, and operating effort. | The revised article omits these unestablished measurements and retains the verified storage and worker figures. |
| Scope | Carefully limited to currently onboarded ML, streaming, and data-platform workloads. | Proposed as serving Uber's entire logging infrastructure. | The revised article retains the narrower, accurate scope. |
| Reader takeaway | CLP plus Presto makes schema-less logs queryable at scale by avoiding work. | One compact architecture handles ingestion and search from producers to SQL users. | The proposed version is easier to remember, but needs tighter boundaries to avoid becoming two articles joined together. |

## What the revised plan adds

- **A real ingestion story:** why independently deployed producers do not require coordinated schema changes, how recent data becomes searchable, and how immutable archives are built.
- **A producer-to-user architecture figure:** the present draft begins at the query entry point and does not show the whole data path.
- **A stronger evaluation section:** concurrency and user demand, AI-driven query volume, latency, and operational burden—not only storage reduction and worker utilization.
- **A sharper connector claim:** a row-oriented compatibility adapter would preserve SQL syntax but lose CLP's encoded, columnar execution and cause avoidable data expansion.

## What the previous draft contained that the revision must preserve

These ideas should remain, even if shortened:

- One SQL entry point and metadata-driven storage selection.
- One query spanning recent KV-IR files and immutable archives without duplicate events.
- Dynamic exact-path and wildcard access for polymorphic log fields.
- Split pruning, correct newest-first Top-N early termination, and direct object-storage reads.
- Pushdown into CLP and in-process execution inside Presto Native's Velox workers.
- Clear boundaries among open-source connector capabilities, Uber production extensions, and future work.

Without these details, the article would state that the system scales without adequately explaining why.

## Proposed claims intentionally omitted

The source material supports approximately 25 PB/month from **onboarded workloads**, 750–850 TB/month compressed, five 48-core workers at roughly 5–10% CPU, and recent-data freshness of approximately 30 seconds to a few minutes. The revised article does not add the following proposed claims:

- Coverage of Uber's entire logging infrastructure or a numerical producer count.
- A 30-second-to-one-minute freshness range, which is narrower than the approved 30-seconds-to-a-few-minutes range.
- Hundreds of users per week, thousands of queries per day, or quantified AI/MCP traffic.
- Universal few-second latency or a p99 latency value.
- A numerical operator or maintainer count.
- Quantified savings from metadata beyond timestamp filtering or quantified acceleration over a non-native implementation.

The draft explains metadata and encoded execution causally instead. It also uses **CLP**, not “CLP-S,” and avoids “full CLP speed” without a controlled benchmark.

## Implemented synthesis

Use the three takeaways, but preserve one causal spine: **many evolving producers create a schema-coordination problem; CLP preserves their structure during ingestion; the Presto connector exposes that structure without discarding CLP's native execution model; metadata and encoded execution avoid enough work to keep the query tier small.**

The revised draft uses this section order:

1. **Production result and question** — 25 PB/month, compressed footprint, five workers; clarify that queries scan only relevant subsets.
2. **Why logs resist a fixed table schema** — dynamic fields and type polymorphism; introduce the three system requirements.
3. **Ingestion without producer coordination** — verified producer-to-KV-IR-to-archive flow, freshness, compression, and immutability.
4. **One SQL entry point** — routing, recent/archived overlap, and duplicate suppression.
5. **A native CLP connector for Presto** — dynamic SQL access, split-local structure, pushdown, and in-process Velox execution.
6. **Avoiding work at 25 PB/month** — metadata pruning, Top-N early termination, and direct object-storage reads.
7. **Measured production impact and open source** — verified usage, latency, resource, and maintenance figures; capability boundaries and attribution.

This version makes ingestion important without letting the article alternate repeatedly between ingestion and query mechanics. It also gives the connector a clear role: it is the bridge between CLP's ingestion/storage model and Presto's SQL ecosystem, not a standalone feature tour.
