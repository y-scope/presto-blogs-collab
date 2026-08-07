# Bringing Schema-Less Data Analytics to Presto Native with CLP

**Document status:** Recommended publication outline  
**Audience:** Presto users and contributors familiar with Hive, Iceberg, catalogs, splits, and pushdown  
**Target length:** 2,700–3,100 words  
**Primary goal:** Explain the CLP connector while teaching a reusable pattern for external Presto-native connectors  
**Secondary goal:** Establish production credibility and leave readers excited about metadata-driven intelligence over evolving data

---

## Story Summary

Presto users already know the connector pattern: a catalog provides a table schema, the coordinator plans splits, and workers scan files such as Parquet or ORC. CLP initially appears to fit this model—until schema-less data violates its central assumption. Fields and types evolve continuously in producer applications, often without coordination with the Presto operators running the query infrastructure. Users may not know which keys they need, one path may have several types, and CLP-S arrays may contain recursively nested values of different types. Several essential search and projection operations therefore cannot be expressed through standard schema-bound SQL. Logs are the production case study and an especially strong use case, not a restriction of the connector model.

CLP solves the storage side by losslessly compressing the log patterns and JSON structures present within each archive/split into a representation that remains directly searchable. Every archive contains the structure needed to interpret its own events, so different splits may introduce different fields and types. A deployment may derive an optional global view that exposes common fields as ordinary Presto columns, but querying does not depend on that view: schema-aware columns and pure schema-less UDF access coexist. Because CLP-S is implemented in C++, it is a natural fit for Presto Native. But integrating it required two deeper contributions:

1. Completing important infrastructure for independently developed, dynamically loaded C++ connectors.
2. Extending the SQL query surface so familiar columns and schema-less path, value, type, and complete-event operations can coexist and be pushed into CLP-S.

The article then shows why this flexibility remains efficient: metadata avoids unnecessary archives and splits; CLP-S searches the remaining encoded data without reconstructing every raw event. The public connector already supports dynamic field exposure, schema-less query rewriting and pushdown, polymorphic typed access, and complete-event projection. Because Presto currently has `JSON` but no first-class `VARIANT`, typed functions provide planning-time types while JSON provides the complete-event compatibility surface. A future Variant could improve type-agnostic access to a known path, but would not solve field discovery or wildcard-key access; CLP's split-local schema tree addresses that complementary problem. YScope and Uber worked together to extend and apply the foundation in Uber production, adding planning-time early termination and advanced metadata filtering and planning at billion-file scale. Finally, the article uses CLP as a practical guide for building and shipping another external native connector and hints at future summarization and semantic-search capabilities.

### Narrative chain

1. **Familiar model:** Hive and Iceberg teach us how Presto normally sees data.
2. **Broken assumption:** Schema-less data has no required stable schema, and the catalog cannot define the full query surface.
3. **Storage answer:** CLP compresses repeated structure into a searchable, split-local representation, often many times smaller than Parquet for logs.
4. **Native answer:** CLP-S can be loaded directly into Presto-native workers.
5. **Language answer:** Typed CLP functions bridge Presto's current lack of Variant, while complete-event JSON preserves arbitrary structure and CLP metadata reaches beyond known paths.
6. **Performance answer:** Metadata avoids work; CLP-S efficiently executes what remains.
7. **Community value:** The connector is usable today and provides a reference path for other native plugins.
8. **Future:** Compression metadata can support more than scans.

---

## Reader Promise

By the end, readers should understand:

- What CLP is and why searchable compression is relevant to Presto.
- Why logs are a compelling example of a broader schema-less and semi-structured data problem.
- How the CLP connector resembles Hive/Iceberg connectors.
- Where that analogy breaks for schema-less and polymorphic data.
- How an external C++ connector spans the Java coordinator and native workers.
- Why the connector needed new Presto-native infrastructure.
- How schema-aware SQL and schema-less functions coexist.
- Why these functions are a language extension, not JSON-parsing convenience wrappers.
- Why Presto's current `JSON`, `ROW`, and `ARRAY(T)` types do not fully represent CLP-S's recursive polymorphism.
- What a future Variant type would solve—and why unknown-key and wildcard-field operations would still require CLP semantics.
- How metadata pruning and native encoded search combine.
- How to build, package, deploy, and validate an external connector.
- What is open source today, what Uber adds internally, and what the architecture may enable next.

---

## Working Title Options

### Recommended

**Bringing Schema-Less Data Analytics to Presto Native with CLP**

- Leads with the user-visible capability.
- Names both Presto Native and CLP.
- Broad enough to include plugin infrastructure and query semantics.

### Connector-author emphasis

**Beyond Fixed Schemas: Building an External Connector for Presto Native**

- Strong for contributors.
- Undersells searchable compression and the CLP name.

### Educational emphasis

**Building and Shipping a Presto-Native Connector: Lessons from CLP**

- Makes the how-to value explicit.
- Makes schema-less query semantics feel secondary.

### More provocative

**SQL Without a Fixed Schema: Inside the CLP Connector for Presto Native**

- Highlights the intellectual contribution.
- Must be carefully worded so it does not imply that Presto itself has abandoned planning-time types.

---

## Opening: When Schema-Less Data Stops Behaving Like a Table

**Target:** 200–250 words

### Hook

- Begin from the Presto reader's mental model, not Uber history.
- Hive and Iceberg connectors expose known columns, use metadata to find files, and schedule splits.
- Establish logs as the concrete production example of a broader schema-less data problem.
- Ask what happens when:
  - no producer-defined schema is required;
  - fields appear immediately after deployments;
  - the same path holds different types;
  - a nested array contains elements of different types;
  - users need to access unexposed paths or search values without knowing their keys;
  - a complete event must be returned even though the planner does not know its structure.

### Thesis

- This is not solved by placing every event in a JSON string.
- It requires:
  - a storage system that retains searchable structure;
  - a native connector path;
  - schema-less query semantics within SQL;
  - pushdown so flexibility does not become full-document parsing.

### Suggested opening formulation

> Presto connectors usually begin with a reassuring assumption: before a query runs, the catalog can tell the planner what columns exist and what types they have. Logs do not always offer that reassurance.

### Transition

> Before looking at the connector, it helps to understand the storage engine it brings into Presto.

---

## 1. CLP in Two Minutes

**Target:** 250–300 words

### Purpose

- Most readers will not know CLP.
- Explain only the compression/search properties needed for the connector story.

### Content

- CLP is an open-source system for losslessly compressing and searching data with evolving structure.
- Its schema-less connector model applies to semi-structured data broadly; logs are especially favorable because of repeated patterns, polymorphism, evolution, and scale.
- It supports:
  - message-oriented logs;
  - structured envelopes containing natural-language messages;
  - full JSON event logs;
  - modern execution traces, such as coding-agent runs containing tool calls, structured state, and natural-language output.
- Show a tiny repeated-message example:

~~~text
Request 123 failed after 45 ms
Request 987 failed after 12 ms
~~~

- Explain storing the common pattern once and encoding variables separately.
- For JSON:
  - recurring paths and structures are represented compactly;
  - observed types are retained;
  - object values and array elements may be recursively nested and heterogeneous;
  - structure does not need to be repeated in every event.
- Each split carries or references the structural information needed to interpret its events.
- A new split can introduce fields or types without mutating one global schema contract.
- A deployment may optionally derive a global view of common fields for discovery and conventional SQL; it is not required for schema-less access.
- CLP-S can search using split-local structure, logtypes, typed variables, and archive statistics.
- It does not need to fully reconstruct every raw event before filtering.
- Add one controlled comparison over the same losslessly represented corpus:
  - CLP-S output: **[CLP-S SIZE]**;
  - Parquet with **[CODEC/SETTINGS]**: **[PARQUET SIZE]**;
  - result: CLP-S is **[X]× smaller than Parquet**.
- Prefer “X× smaller compressed footprint” over the ambiguous “X× better compression ratio.”
- CLP-S is C++.
- Link the OSDI '24 μSlope paper for algorithms and evaluation.
- Attribute the paper to YScope, the University of Toronto, and Uber.

### Avoid

- Deep compression ratios from the paper.
- Storage-layout pseudocode.
- A complete history of CLP.
- Conflating the paper's broad Uber volume with the current production deployment.

### Transition

> CLP already knows how to search evolving logs. The next question is how that capability fits into Presto's familiar connector model.

---

## 2. Familiar Architecture, Different Data Contract

**Target:** 350–450 words plus one diagram

### Start with Hive/Iceberg similarities

- Catalog exposes datasets as tables.
- Coordinator discovers metadata.
- Connector plans splits.
- Metadata prunes irrelevant data.
- Workers scan in parallel.
- Filters and projections are pushed toward storage.
- Data can remain in object storage.

### Compact comparison

| Hive/Iceberg concept | CLP analogue |
|---|---|
| Catalog/metastore | Pluggable CLP metadata provider |
| Registered table schema | Optional global view of common discovered fields, plus schema-less access |
| Parquet/ORC data file | CLP-S archive |
| File/row-group split | CLP archive split |
| File reader | Native CLP-S search and projection |
| Partition/manifest pruning | Metadata-level archive pruning |

### Explain where the analogy breaks

- No producer- or administrator-defined schema is required.
- Structure is fundamentally represented per split rather than imposed as one required global schema.
- An optional global view can expose selected common fields as ordinary columns.
- Those columns are not the full query surface.
- Dynamic key, value, type, path, and complete-event access remain available.
- A conventional Presto `ROW` still has a fixed planned shape, and `ARRAY(T)` still has one planned element type; neither is a universal representation for CLP-S's recursive polymorphism.
- Workload is read-only and append-oriented.
- Physical reader understands log patterns and encoded structure.

### Architecture diagram

Show:

1. SQL clients.
2. Java coordinator.
3. Coordinator-side CLP metadata and split planning.
4. Presto-native workers.
5. Native CLP connector loaded as a shared library.
6. Pluggable metadata provider containing archives, statistics, and an optional global field view.
7. CLP-S archives in filesystem or object storage, each with its split-local structure.

Optional dashed callout:

- Uber production extensions: internal metadata, routing, planning, object storage, and operational controls.

### Core line

> The boxes look familiar. The difference is that each CLP split describes its own structure, while an optional global view of common columns remains a convenience rather than a boundary around what can be queried.

### Transition

> Preserving CLP's native execution required more than implementing the usual metadata interfaces.

---

## 3. A Native Engine Inside a Native Engine

**Target:** 400–500 words

### Why a true native connector

- CLP-S is already C++.
- Presto-native workers execute in C++.
- Link CLP-S into the worker as a shared library.
- Keep schema navigation, encoded search, filtering, and projection in the same native process.
- Avoid:
  - sidecar service;
  - JNI scan path;
  - extra RPC boundary;
  - Java rewrite of CLP-S;
  - raw-event materialization before search.

### Contrast with connector federation

- Remote database connectors may be network/protocol bound.
- Reusing a mature Java connector can be appropriate there.
- CLP is a specialized storage scan whose reader/search code is the performance-critical path.
- Native integration preserves its value.

### Presto-native gap

An external connector needs:

- Dynamic shared-library loading.
- Connector-factory registration.
- Coordinator-created connector-specific handles.
- Serialization through the Java plan.
- Routing/deserialization on C++ workers.

### Upstream work

- Link PR #27653.
- Link PR #27652.
- Explain their roles in one sentence each.
- State that they completed important missing infrastructure, not that they alone created all plugin loading.
- Present CLP as an early real external/reference connector.

### Attribution

- YScope engineers: primary connector contributors and named upstream changes.
- IBM and Presto community: surrounding infrastructure, guidance, and review.
- Deepak Majeti: significant review if individual naming is approved.
- Uber: production design/deployment partner, not currently a named author of the two dynamic-loading PRs.

### Core line

> CLP was not rewritten for Presto Native. Presto Native learned how to load CLP's existing C++ engine as an independently developed connector.

### Transition

> Native loading solves how the worker executes CLP. It does not solve how a SQL query describes data whose fields are not known in advance.

---

## 4. SQL Where the Schema Ends

**Target:** 550–650 words with two SQL examples

### Establish coexistence

- Avoid describing schema-aware and schema-less as mutually exclusive modes.
- They are composable access paths.

#### Schema-aware

- Common discovered fields behave like ordinary SQL columns.
- Familiar for timestamp, service, severity, host, and other repeated dimensions.
- Supports ordinary joins, aggregation, ordering, grouping, and projection.

#### Schema-less

- Functions address unknown, uncommon, deeply nested, newly introduced, special-character, or polymorphic fields.
- Same query can use both.

### Explain the expressiveness gap

Standard relational SQL cannot naturally:

- search all values without enumerating columns;
- address a known path that was never exposed as a planned column;
- project a field unknown during planning;
- return a complete event outside the table's planned structure;
- select one runtime type from a polymorphic field.
- return a nested heterogeneous array through conventional `ARRAY(T)` without coercion or type erasure.

Be explicit:

- SQL LIKE over a known column is not equivalent.
- Generic JSON functions are extensions over an opaque document, not a storage-native answer.

### Explain the current Presto type boundary

- Presto 0.298.1 has a `JSON` type but no first-class `VARIANT` type.
- `JSON` can preserve JSON-level mixed values, but the planner sees one JSON expression type and typed scalar use requires extraction/casting.
- `ROW` requires a planner-known shape.
- `ARRAY(T)` requires a planner-known element type, while CLP-S arrays may mix scalars, objects, nested arrays, and nulls.
- Schema and type changes originate with producer applications and may occur without catalog migrations or coordination with Presto operators.
- Typed CLP accessors therefore give Presto a stable result type while resolving the requested path against split-local structure.
- Complete-event JSON is the currently verified type-agnostic projection; do not imply the connector constructs a dynamic Presto `ROW`.

### Function mechanics

- Use the verified public API, not illustrative RFC names:
  - `CLP_GET_BIGINT(path)`;
  - `CLP_GET_DOUBLE(path)`;
  - `CLP_GET_BOOL(path)`;
  - `CLP_GET_STRING(path)`;
  - `CLP_GET_STRING_ARRAY(path)`;
  - `CLP_GET_JSON_STRING()` for the complete event;
  - typed `CLP_WILDCARD_*_COLUMN()` functions for value searches across fields.
- Paths use constant strings; public tests demonstrate names such as `user_id` and dotted paths such as `city.Name`.
- Typed functions let the planner understand the expression while CLP resolves the path against each split's structure.
- Same function may appear in SELECT and WHERE.
- Search and projection are not necessarily distinct UDF families.
- The registered Java functions are planner placeholders, not record-at-a-time implementations; the connector rewrites them into scan-level operations.

### Example 1: Familiar SQL

- Timestamp/service/severity filter.
- Projection and aggregation using exposed columns.
- Demonstrates low learning cost.

### Example 2: Combined access

- Common columns for coarse filtering.
- Typed dynamic projection.
- Typed wildcard value search using the verified public function names.
- Use a publicly supported predicate pattern; do not show a two-sided `LIKE '%value%'` unless that path is verified for the release being discussed.
- Optionally include `CLP_GET_JSON_STRING()` to demonstrate complete-event projection explicitly.
- ORDER BY/LIMIT or aggregation.

### Pushdown

- Functions are not ordinary record-at-a-time JSON UDFs.
- Connector recognizes supported expressions.
- Query intent is translated into connector-specific operations.
- Native worker calls CLP-S.
- Residual unsupported expressions remain in Presto.

### Complete-event terminology

- Say “complete log event,” not “entire row.”
- Be specific that `CLP_GET_JSON_STRING()` returns the complete event in JSON form.
- Do not imply that CLP constructs a dynamic Presto `ROW` containing every possible field.

### Brief future-facing distinction: Variant versus wildcard fields

Keep this to one compact paragraph in the draft:

- A future Presto `VARIANT` type could support an additional exact-path accessor that preserves the runtime type, including nested objects and heterogeneous arrays.
- Do not propose changing the return type of existing UDFs; a new Variant-returning function would avoid breaking queries while typed accessors remain useful.
- Variant solves type uncertainty at a known path, not key uncertainty.
- Dremio's documented `VARIANT_GET` illustrates the distinction: it can return Variant or cast an explicit path, but its documented path syntax does not support wildcards.
- CLP's split-local schema tree creates a path toward wildcard-field projection or search without scanning every field name in every reconstructed event.
- A future multi-match projection should preserve both paths and values, potentially as one nested Variant containing an array of `{path, value}` objects.
- Clearly mark wildcard-field projection as future design direction; the verified public API currently provides typed wildcard **value search across fields**.

### Memorable line

> CLP does not merely make schema-less data easier to query with SQL; it makes schema-less operations expressible within a SQL query.

### Transition

> Expressiveness only matters at log scale if the connector can preserve the storage engine's ability to avoid and execute work efficiently.

---

## 5. Avoid Work, Then Search the Rest in Compressed Form

**Target:** 400–500 words

### Two-stage model

1. Metadata prevents unnecessary work.
2. CLP-S efficiently executes the remaining work.

### Metadata pruning

- Pluggable metadata and split-provider interfaces.
- The CLP open-source project includes a basic MySQL-backed reference provider.
- Uber and YScope use more advanced internal metastore implementations for larger production deployments.
- Metadata may include:
  - archive location;
  - time bounds;
  - selected indexed dimensions;
  - an optional global view of common fields;
  - counts/statistics.
- Split-local structure is intrinsic to each archive; it does not have to be centralized in the metastore for schema-less access.
- Translate supported top-level filters.
- Generate splits only for a safe candidate set.

### Native encoded execution

- Worker streams or opens the selected archive.
- CLP-S uses schema/logtype/typed-value information.
- Search encoded regions.
- Decode or reconstruct only likely matches and requested projections.
- Results flow into Presto vectors/operators.

### Object storage

- High compression lowers transferred bytes.
- Streaming begins as data arrives.
- Uber searches directly from object storage with no archive cache or local-disk staging.
- Avoid framing object-storage API pacing as a performance defect:
  - it is deliberate shared-resource governance;
  - queries already meet UX requirements.

### Uber planning extension

Mention in one compact paragraph:

- Metadata pruning and planning-time early termination are orthogonal.
- Early termination is not a generic optimization for arbitrary `ORDER BY ... LIMIT` queries.
- Eligibility relies on:
  - a finite `LIMIT N`;
  - ordering by the log timestamp, normally newest first;
  - append-only logs;
  - internally time-ordered archives with trustworthy bounds;
  - time-aware metadata enumeration;
  - proof that unseen older archives cannot affect the requested result.
- These conditions describe the dominant interactive logging query—“show me the newest N events matching these filters”—so most logging investigations can benefit.
- After applying compatible top-level filters, ordered streaming metadata can stop split generation once the proof is satisfied.
- Presto's scheduler remains unchanged and may execute selected splits in any order.
- Explain the eligibility and correctness intuition at this high level only. Do not link or cite the internal early-termination design materials; they are not intended for public consumption.

### Core line

> Metadata determines what not to read; CLP-S makes what remains inexpensive to search.

### Transition

> The same architecture also provides a concrete playbook for other native connector authors.

---

## 6. From Shared Library to Running Catalog

**Target:** 450–550 words

### Educational purpose

Use CLP as a reference implementation, not merely a case study.

### Five-step lifecycle

#### 1. Define the connector contract

- Tables and metadata.
- Column/type model.
- Splits.
- Pushdown.
- Worker result production.

#### 2. Divide coordinator and worker responsibilities

- Java: discovery, planning, split/handle creation.
- C++: registration, deserialization, native scan, result vectors.

#### 3. Build and package

- Coordinator artifact.
- Native shared library.
- CLP-S native dependencies.
- Compatible Presto/Velox version.
- Build against a manylinux_2_28 baseline for Linux distribution compatibility.
- Bundle transitive shared libraries and adjust RPATH so packaged dependencies resolve at runtime.
- Unit plus cross-boundary tests.

#### 4. Deploy and register

- Install coordinator component.
- Install/mount native library on workers.
- Configure native plugin loading.
- Add catalog properties.
- Configure metadata/split providers.

#### 5. Validate and publish

- SHOW TABLES.
- DESCRIBE.
- Schema-aware query.
- Schema-less query.
- Pushdown verification.
- Versioned artifacts.
- Compatibility documentation.
- Release notes for serialization/ABI changes.

### Practical public path

- Link the connector repository.
- Link the main CLP repository and explain that it provides a minimal end-to-end Presto-native connector setup.
- Link its Docker Compose and Kubernetes paths.
- State that the integration is usable today.
- Do not include unverified compiler commands in the article.
- Include one minimal configuration excerpt after checking it against the release being discussed.

### Community value

- Domain teams can own connectors outside the monorepo.
- Presto provides an extension boundary.
- Specialized storage engines retain native execution.
- CLP becomes an implementation reference.

### Transition

> The open-source path is not theoretical; YScope and Uber extended and applied the same foundation in Uber production.

---

## 7. Open Source Today, Production Validated, Built for More

**Target:** 400–500 words including conclusion

### Open source today

- Connector is public and integrated with CLP.
- End-to-end deployment is available.
- Already supports:
  - optional dynamic field exposure;
  - schema-less `CLP_GET_*` query rewriting;
  - native filter and projection pushdown;
  - polymorphic typed access;
  - typed wildcard value searches across fields;
  - complete-event projection through `CLP_GET_JSON_STRING()`;
  - basic pluggable metadata and split filtering.
- Do not frame it as a skeleton.

### Uber production role

- Principal production design and deployment partner.
- Clearly identify the capabilities not currently in OSS:
  - planning-time early termination;
  - advanced metadata filtering and planning;
  - billion-file metadata scale.
- Other Uber-specific integration:
  - object storage;
  - routing;
  - reliability/controls.

### Only the strongest scale proof

Recommended figures:

- Approximately 25 PB/month raw onboarded logs.
- Approximately 750–850 TB/month compressed.
- Approximately 29–33× reduction from raw input, depending on log mix.
- Five 48-core native workers.
- Direct object-storage reads with no local cache.

Optional:

- Typical 5–10% CPU if measurement window is approved.

Do not add a large benchmark table unless one representative query produces verified, consistently scoped values.

### Open-source direction

- Uber and YScope intend to upstream generally useful components.
- No promise of feature list or timing.

### Future: metadata as a query substrate

- CLP already records schema, log patterns, types, and statistics.
- This can support:
  - compact dataset summaries;
  - pattern/schema changes;
  - candidate narrowing for semantic search.
- It can also support future wildcard-field discovery and projection, complementing a possible Presto Variant type: Variant preserves the type of a known value, while the schema tree helps find fields whose names are unknown.
- Avoid unverified “pennies/no GPU” claim.
- End on architectural possibility, not product promise.

### Conclusion

Recap:

- Presto gains a practical external native connector pattern.
- CLP gains SQL composition without a required schema.
- Common fields remain familiar.
- Schema-less functions extend expressiveness.
- Metadata and native encoded execution preserve efficiency.

### Proposed final line

> CLP expands both what Presto can connect to and what a Presto query can express.

---

## Required Visuals and Examples

### One architecture diagram

- Java coordinator versus native worker boundary.
- External shared library.
- Metadata provider.
- CLP-S archives.
- Optional Uber extension callout.

### One compact comparison

- Hive/Iceberg concept to CLP analogue.
- No more than six rows.

### Two SQL examples

1. Ordinary exposed-field query.
2. Combined ordinary and schema-less query.

### One build/deploy checklist

- Prefer numbered lifecycle.
- Avoid a second large architecture diagram.

### Optional production callout

- Four concise numbers, not a dashboard.

---

## Claims to Resolve Before Final Publication

- Exact Parquet comparison corpus, codec/settings, sizes, and resulting multiplier.
- Whether to include two-sided wildcard/grep examples beyond the publicly tested predicate shapes.
- Exact Java/C++ responsibility split.
- Exact build and artifact publication path.
- Compatibility/version requirements.
- Whether “one of the first” is approved.
- Final named contributor acknowledgments.
- Production metric windows.
- Final wording and eligibility conditions for Uber's internal early-termination optimization.
- Whether semantic-search direction is publishable.
- Whether and how to mention the future Variant/wildcard-field API direction without implying an accepted Presto proposal or a currently implemented CLP function.

---

## Material Explicitly Out of Scope

- Uber's full log-ingestion platform.
- Pinot hot-tier architecture.
- Detailed CLP-S compression format.
- Full μSlope evaluation.
- Formal early-termination derivation.
- Complete metastore design.
- Exhaustive UDF reference.
- Full build troubleshooting.
- Edge/laptop execution unless needed as one closing sentence.
- Unverified partner or semantic-search performance claims.
