# CLP Part III: Editorial Pitch and Reader Review

## The 1-Pager Pitch

**Storyline**
- Independently deployed apps can't share a schema. Fields shift shape and type without warning.
- CLP discovers structure as events arrive. No schema required upfront, no predefined field list.
- Fresh events land in an appendable, searchable KV-IR stream. Closed batches later consolidate into denser, faster archives, on their own schedule. Neither blocks the other.
- Presto exposes both forms as one table, via typed functions instead of a fixed schema.
- Three layers cut work before a worker opens a file: prune files that can't match, skip older files latest-N planning proves it doesn't need, narrow further inside CLP with its schema tree and dictionaries.
- Result: 25 PB of logs a month, five workers, because almost none of that volume becomes query work.
- Closing move: extend the same principle one layer further, giving an agent a complete, small answer instead of a biased sample, then past logs entirely to any telemetry that shares the same problem of unpredictable structure, including the **wide event** (logs, traces, and metrics fused into one record). An earlier plan closed on storage-placement decisions instead. That direction was cut for length so the agent payoff would not have to share the section.

**Why we write this way**
- Big claims get a reversal, not a straight statement: the obvious latest-N stopping rule looks right and isn't, semantic search over raw logs looks necessary and isn't feasible at this scale.
- Let the reader almost believe the wrong answer before showing exactly where it breaks. Real intellectual moment, not a rhetorical trick.
- Every claim scoped precisely: shipped in open source, built by Uber in production, or still just a designed interface.
- Clearest example: the three proposed agent-facing SQL functions (list paths and types, group by recurring format, search by meaning). "The interface is designed. What it still needs is refinement and validation" is the literal state of that work, not a hedge for legal.
- Every future claim grounded in something that already exists: labeling grounded in one format string plus real values, semantic search grounded in logtypes already deduplicated into a small, embeddable set, and the wide-event scope extension grounded in the same schema-materialization property that already handles logs, not a new claim about traces or metrics.
- No leap of faith anywhere. That's what keeps a one-to-two-year horizon credible instead of aspirational.

**Restrictions**
- Every production number traces to an approved figure backed by something real, a dashboard, a screenshot, an internal query result, not just a verbal sign-off. The two aggregate ones also carry a footnote defining exactly how they were computed.
- Nothing about Uber's other storage systems or any named competitor in the visible article. The hot-tier and Parquet Variant comparison lives in a footnote only.
- AI section: no cost superlatives until workload, denominator, and baseline are defined. No "for pennies." No claiming something is deployed before it's validated. Feasibility is explained, not asserted.
- "First" claims hedged to "one of the first" until chronology is fully verified. One spot in the current draft still needs that fix.
- Sentence level: no em dashes, no semicolons in prose, no second person.

**Tone**
- Never tell the reader to be impressed. State the fact, let them get there themselves.
- No struggle narrative anywhere. Every section describes a finished, working system, not a journey to one.
- Three reasons: this is Uber's official blog under real review, an earlier logging blog was already rejected for reading like a Pinot advertisement, and it's simply how someone writes when overclaiming carries real, personal consequences.

**YScope's angle**
- The real audience isn't only Uber. It's anyone with their own petabyte-scale, schema-less logging problem.
- Every hedge and every footnote is also evidence: YScope says exactly what it knows and no more, and the five years behind this piece went into production, not just a paper.
- The storage table in section one puts CLP at the center and measures Uber's own hot-tier storage and Parquet Variant against it, not the other way around. That's the mechanism for becoming the reference point a landscape gets compared to, not a vendor asking for a seat at someone else's table.
- None of this is stated as a claim about YScope. It's what a reader concludes on their own, and industry leadership earned that way outlasts industry leadership announced in a sentence.

## Pros, Weaknesses, Strategy

**Pros**
- Every claim a skeptical reader can check holds up: scoped precisely as open source, Uber production, or still designed, hedged where genuinely uncertain, footnoted where a number needs defending.
- The two strongest passages, the latest-N callout and the `SEMANTIC()` cost argument, are real technical arguments, not marketing lines. That is what earns the 9s from the CTO, Senior Dev, YScope, and Head of AI.
- Production evidence is current and real: 25 PB a month, five workers, agents already the majority of queries. Not a roadmap pitch, and that stat now opens the agent section instead of arriving mid-paragraph.
- Multi-org attribution is specific and accurate, serving the CTO's collaboration priority and every named team's own read of the piece at once.
- Reads as engineering, not marketing, which is the actual strategy for winning YScope's next customer and for not repeating the earlier Pinot-blog mistake.

**Weaknesses**
- Density costs two readers outright. The CEO and the casual outsider both bail before reaching the piece's best material, because nothing in it recruits a reader who doesn't already have the question.
- Two precision items are still open: "the first end-to-end pluggable connector" isn't hedged to "one of the first," and two `PRESTO_BLOG_URL` placeholders depend on a companion post that hasn't shipped.
- The labeling QA and review loop stays implicit, the one place a genuinely skeptical AI reader could still push.
- No struggle narrative anywhere. Deliberate, but it is the ceiling on how shareable this piece can be outside the readers it is actually written for.

**Strategy**
- Close the two open precision items before publication. Cheap, mechanical, and the only thing holding the CTO's score below a clean number.
- Do not chase the CEO or casual-reader gap by simplifying the whole piece. That would blunt exactly what earns trust from the CTO, Senior Dev, YScope, and Head of AI, the readers who actually decide whether this ships and whether it wins anyone real. Bolding the agent-majority stat, then moving it to open the section, was the right size of fix for that gap. A bigger one is not.
- Leave the junior-dev gap alone. That reader was never the intended audience, and simplifying for them would cost precision the intended audience needs.
- Leave the missing war story alone too. It is not an oversight. This piece sits closer to a systems paper translated for engineers than a narrative blog, and precision is the differentiator that genre rewards, not a story arc.
- Cut the storage-tiering and archive-grouping direction that once shared "What Comes Next" with the agent section. Word budget was already tight, and the two ideas were competing for the same closing attention rather than reinforcing each other. The agent section absorbed the one piece of that material worth keeping, the metadata-driven scope extension into wide events, as its own closing paragraph instead.

## Reader Perspectives on the Current Draft

Ratings out of 10 from the perspective of different readers, each judged by what that reader would actually notice and care about, not a single uniform quality bar.

| Reader | Score | Why |
|---|---:|---|
| **CEO** | 8/10 | The intro sells exactly what a CEO skims for: five workers, 25 PB/month, hundreds of users, thousands of queries a day, with an agent-payoff teaser in its last line. Results in Production now reads like an exec one-pager too, with WAU as a metric this reader recognizes instantly, and the conclusion closes on a properly hedged agent claim instead of an unhedged one. Still capped below 9 because the piece's best material, the latest-N callout and the embedding-cost argument, lives in the middle, and a CEO-level skim never reaches it. |
| **CTO** | 9/10 | All four stated priorities are satisfied and the metrics are signed off, so that's no longer an open question. The wide-event line closing the agent section is the clearest answer yet to the collaboration and investment-signal priority: it names the term without a second heading or a hype claim. Held at 9 rather than 10 by two small remaining items: "the first end-to-end pluggable connector" isn't hedged to "one of the first," and two `PRESTO_BLOG_URL` placeholders still depend on the companion post shipping. |
| **Presto Team Manager** | 6/10 | Uber's Presto team gets one generic credit line, "worked with YScope on connector infrastructure," next to Observability's more specific "shaped production requirements and workflows and operates the query service" and Data Storage's "collaborated on the metastore and object-storage paths used for planning, consolidation, and query execution." The upstream-PR paragraph, the one place squarely about Presto Native internals, credits only YScope, IBM, and the broader PrestoDB community, leaving Uber's own Presto team absent from its own subject matter. That absence sits beside the still-unhedged "first end-to-end pluggable connector" claim and both `PRESTO_BLOG_URL` placeholders, so both open accuracy risks in the piece land inside Presto's domain without the team sharing the byline there. Presto is at least named first among the three collaborating teams in the opening credit sentence, which keeps this off the floor. |
| **Observability Team Manager** | 6.5/10 | Observability's only credit line, "shaped production requirements and workflows and operates the query service," compresses two different roles into one clause, while Data Storage gets a dedicated sentence tied to "the metastore and object-storage paths," a mechanism that then gets pages of technical spotlight in the latest-N and pruning sections. The article's hook question, how a five-node cluster serves this workload, is answered almost entirely through architecture: Results in Production covers the response-time numbers, the deliberately limited read rate and parallelism, and "a few engineers maintain the system," without ever attaching Observability's name to any of it. "Shaped production requirements and workflows" never gets unpacked elsewhere the way the metastore work does, so this reader's day-to-day operational role reads as acknowledged but not woven into the narrative it is actually central to. |
| **Senior Dev** (Presto/observability peer) | 9/10 | Exactly who this is written for. The latest-N callout is the kind of thing that gets forwarded to a channel, and the compression footnote choosing to report a lower blended 97% instead of the flashier 169x Spark-only figure is exactly the transparency this audience rewards. Held to 9, not higher, for two reasons: connector internals are twice pointed to the still-unlinked companion PrestoDB post right where this reader is most engaged, and the unhedged "first end-to-end pluggable connector" claim is a line a Presto-community peer is positioned to want qualified. |
| **Junior Dev** | 6/10 | The hook lands for anyone. Schema tree, watermark, guaranteed-vs-approximate mode, and the embedding-cost argument for `SEMANTIC()` all assume intuition this reader may not have yet. Terms are defined at first use, but density compounds. Not written for them, and doesn't pretend to be. |
| **YScope** | 9/10 | Named first, ahead of Uber's own teams, as technical lead on the storage engine, the connector, and the upstream Presto infrastructure. Collaboration ties YScope by name to the article's single most technically impressive mechanism, the guaranteed-mode watermark logic, not just to storage compression, and the upstream PR credit is concrete and linked. The one friction point for an accuracy-minded reader: the still-unhedged "first end-to-end pluggable connector" claim is a superlative attached to YScope's own work, exactly the kind of claim this persona would want walked back before publication. |
| **Outsider** (Presto/systems community) | 8/10 | Concrete numbers, a real deployment, an instructive correctness argument in the latest-N section, and linked upstream PrestoDB PRs with named reviewers from IBM and Meta: checkable, non-marketing evidence of Presto-community engagement. Kept below 9 by two things this exact audience would notice: the deepest connector mechanics are still deferred to an unlinked companion post, and "the first end-to-end pluggable connector for Presto Native" is an unqualified superlative in this reader's own domain. |
| **Outsider** (casual technical reader) | 5/10 | Gets the hook, bounces in section 1 or 2 once schema-tree mechanics start. Never reaches the sections that carry the piece. |
| **Head of AI** | 9/10 | The best-aligned reader in the table. The agent-majority stat is real production evidence, not a roadmap promise, and now opens the section instead of arriving late. Both feasibility arguments explain their own cost model instead of one being argued and one asserted: labeling tracks logging statements, `SEMANTIC()` tracks distinct formats, neither tracks event volume. The closing wide-event paragraph extends the vision past logs without adding a single unproven claim, since it leans on the same schema-materialization property already established. No hype language anywhere. Short of a 10 for two small reasons: the labeling QA loop stays implicit, and the majority-of-queries stat itself carries no methodology footnote the way the compression and work-reduction numbers do. |

