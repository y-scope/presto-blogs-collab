# Bringing Native SQL to Petabyte-Scale Semi-Structured Logs: Inside Uber’s CLP and Presto Infrastructure (Part III)

Structural record for the finished draft: what the article is, section by section, plus the constraints a later editor cannot recover by reading it. Pitch, tone, sentence-level restrictions, and reader reception live in `clp-part-iii-review.md` and are not repeated here.

- **Type:** Production architecture case study
- **Length:** 3,821 words as published, against an original target of roughly 3,500. Skimmability was preferred over a rigid cap.
- **Audience:** Presto, observability, storage, data-platform, streaming, machine-learning, and artificial-intelligence engineers
- **Spine:** applications and platform workloads → searchable CLP KV-IR → consolidated columnar CLP archives → one Presto query path → progressively narrower execution

## Standing decisions

- **Cut for length:** the second future-work track, formerly `### Better Storage Decisions from Existing Metadata` (present in commit `ef79b83`, removed after). The published article carries one forward-looking direction, not two. Similarity-aware and query-aware archive grouping, automated tiering, and archive-open reduction are not content in any section, so omit them rather than deferring them. If the track returns, it gets its own section rather than sharing "What Comes Next."
- **Companion PrestoDB post** (`PRESTO_BLOG_URL`) carries the current pluggable connector, CLP versus Variant, and connector-author lessons. This article therefore never repeats the optimizer walkthrough, benchmarks, the Variant comparison, or a connector-author guide.
- **Terminology:** the formal names Merged Schema Tree and Encoded Record Table appear nowhere in the article. It says "schema tree," "internal tables," and "recurring text patterns" instead. Terms it does keep, such as KV-IR and split, are defined through the work they perform.
- **Capability boundary,** distinguished at first mention:
  - *Current open source:* CLP KV-IR and archives, mixed-form queries, typed exact and wildcard paths, typed value search without a known path, complete-event JSON, filters and projections pushed into CLP, pluggable metadata and split filtering.
  - *Uber production:* collection and enrichment, metadata publication, archive orchestration, mixed-form precedence, a metastore managing billions of file records and performing latest-N file skipping, and a query layer handling routing, reliability, and operational controls.
  - *Still designed only:* `SCHEMA()`, `PATTERN()`, `SEMANTIC()`, naming the variables inside a recurring format, extending the same metadata past logs, and selective reload. Never describe any of these as a complete production or open-source workflow.

## 1. Columnar Compression and Search for Evolving Semi-Structured Data

**Purpose:** Establish both hard problems, uncoordinated application-defined structure and query cost at this volume, then CLP's preserved structure.

- The **Four ways to store an evolving event** callout is optional. Removing it must still leave a natural transition into the production data flow.
- `[^hybrid-layouts]` names where the "document plus selected typed columns" row already exists in production (Uber's own hot storage tier) and where Parquet Variant with shredding fits the same row. Do not let it grow into the full Variant comparison.

## 2. Compressed and Queryable from Arrival through Archival

**Purpose:** Explain why freshness matters, what consolidation improves, and how one query spans both forms without duplicates.

- Three deliberate omissions, each a topic a later editor would plausibly add back: incomplete-tail behavior, atomic-replacement mechanics, and the historical move from log-appender compression to host-collector compression, which the query story does not need.
- Direct KV-IR-to-archive consolidation is a reusable CLP capability. The draft describes only the KV-IR path, so guard against a later edit implying that CLP archives can only be built from KV-IR.
- Callout: **KV-IR makes an intermediate representation searchable as it grows**, carrying the massively-parallel-parsing argument for compressing at the host collector.
- **Figure 1** (`figures/clp-storage-flow.svg`), placed immediately after the opening two-form explanation and before the KV-IR callout. Fixed layout: application containers above the host log collector, the KV-IR snapshot and the immutable archive aligned in a right-hand object-storage column, asynchronous consolidation between those two storage states, and the metastore below the collector. The KV-IR read enters consolidation's right edge, the remaining consolidation flows leave its bottom edge, and arrows are horizontal, vertical, or right-angle only. Label the archive-write arrow "Build immutable archive." The metastore's selection of closed KV-IR files must look different from the consolidation worker's physical read. Purple (`#6d5bd0`) for observability services, green (`#2f8578`) for files in object storage, two-item legend across the upper right.
- **Figure inventory:** this is the article's only figure. No second connector-architecture figure or query-path callout in Section 3, and no separate planning/execution figure in Section 4, where the three bold lead-ins provide the overview. `figures/` also holds an unused `clp-query-work-reduction.svg` plus a vertical variant that the draft deliberately does not reference.

## 3. Native Semi-Structured Querying with Presto

**Purpose:** One Presto table, the typed access forms over it, and native execution inside CLP.

- Keep Variant out of this section entirely. Do not reintroduce Iceberg v3, the specification links, the Q3 2026 time qualification, or the tracking issue here.
- `event.request_id` appears twice in the SQL example on purpose, once as an ordinary column and once through an exact-path typed function. It is not a duplicate to delete.

## 4. How Five Workers Serve the Workload: Avoiding Work at Every Layer

**Purpose:** Explain the five-worker result as three progressively finer layers of avoided work.

- The three layers, in order: prune files that cannot match, avoid scheduling older files for latest-N queries, narrow each selected CLP scan.
- Keep the connector's metadata-provider agnosticism out of this section. It belongs to the open-source inventory.
- Latest-N precision, all easy to break in a copy pass:
  - The main text stays mode-neutral. A skipped older file may still contain valid matches, so the main text must not claim that none can enter the requested result.
  - Approximate mode is an independent latency-versus-completeness choice, never a fallback for missing exact counts. The fallback belongs to guaranteed mode alone.
  - Do not imply that application, host, or any other filter dimension becomes the sort key.
  - Omit metadata-maintenance and range-quality commentary from the callout.
- `[^work-reduction]` defines a ratio of summed event counts across the query population. Do not call it an average of per-query ratios, and attribute the reduction to all three layers rather than to pruning or latest-N alone. The exact measured value stays only in the publication-evidence checklist, which is why the draft rounds to "more than 200,000 times."

## 5. Results in Production

**Purpose:** Validate the design as a serving system, not only as a compression result.

- The section opens on storage rather than restating the workload roster the introduction already names and links.
- 750–850 TB against 25 PB is a 29–33× physical reduction. The draft publishes only the derived "about 97% less physical storage," so that ratio appears nowhere in the article itself.

## 6. What Comes Next: Beyond Guesswork for Agents

**Purpose:** Carry the single forward-looking direction, giving agents discovery and retrieval without guessing exact paths or terms.

- Callout "From a vague question to three recurring failure patterns": keep the `clp.coding_agent_sessions` table vendor-neutral, and generalize the three functions to a trace but explicitly not to a metric, since a metric typically has no free text for `PATTERN()` or `SEMANTIC()` to match against.
- The closing paragraph names the **wide event**, logs, traces, and metrics fused into one record per request, and attributes the term to the industry rather than to Uber. Treat the term as load-bearing rather than as a flourish: it is a deliberate signal to Uber leadership. A polish pass once traded it for the shorter "Combining sources" and it had to be restored, so any future trim of that paragraph must keep it.

## 7. Collaboration Across Teams and Companies

**Purpose:** Ground the production system in the teams and companies that built it.

- The claim that the CLP connector is the first end-to-end pluggable connector for Presto Native is verified and ships **unhedged**. Never soften it to "one of the first."

## 8. Conclusion

**Purpose:** Close on the real accomplishment and the one thread the article leaves open.

- No three-part series recap and no "nearly five years" framing here, both of which belong to Section 7. Do not close on a pull-quote. The conclusion paragraph ends directly, with the footnotes immediately after.

## Guardrails

- Scope 25 PB per month to logical uncompressed data from currently onboarded workloads, never to one query or to physical bytes read, and keep raw volume, compressed footprint, and bytes read distinct.
- Keep schemas archive- or split-local, never dataset-global.
- Attribute Uber services, Presto, the connector, and CLP's native storage and search engine precisely. Keep Uber's metastore implementation a black box.
- Do not supply numbers the draft deliberately withholds: the query layer's threshold values in Section 4, and a network and I/O utilization percentage in Section 5.
- Strict third person throughout, a deliberate break from the earlier blogs' "we." No "we," "our," or "us," even for Uber's own design choices. Name the team or the system as the actor.
- Present tense only for behavior that is proven and currently running. Conditional tense (would, could) for the proposed `SCHEMA()`, `PATTERN()`, and `SEMANTIC()` interfaces.
- Carve-outs to the sentence rules in `clp-part-iii-review.md`: one semicolon is allowed where SQL syntax requires it, and en dashes stay in numeric ranges such as 750–850 TB, 5–10%, and 29–33×.

## Before publication

- Replace both `PRESTO_BLOG_URL` placeholders: the connector-internals pointer closing Section 3, and the Variant note inside `[^hybrid-layouts]`.
- Convert the Markdown footnotes to the publisher's native footnote component if required.
- Complete narrative review, then `git diff --check`.
