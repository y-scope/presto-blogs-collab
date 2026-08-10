# The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data

## Article

- **Goal:** Explain specialized storage behind an external Presto Native connector.
- **Audience:** Presto users, operators, maintainers, connector authors, and engineers.
- **Why:** Variant is becoming standard while Presto Native opens external connectors.
- **Takeaway:** Variant standardizes logical/table contracts; CLP preserves native columnar storage and search.

## Opening: A common SQL type is not a storage design

- **Goal:** Establish the problem and historical transition.
- **Why:** The comparison and architecture need a common baseline.
- **Takeaway:** Presto standardizes access, not physical organization.
- **Important points:**
  - Evolving logs defeat fixed schemas; JSON loses field-level columnar execution.
  - [August 2025 YScope post](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82): CLP concepts and fork-based native integration.
  - Current connector: independently developed shared library loaded by Presto Native.
  - PrestoDB lacks end-to-end Variant support.

## 1. Variant as a multi-layer lakehouse baseline

- **Goal:** Explain Variant's SQL, Iceberg, and Parquet layers.
- **Why:** Readers need this standard vocabulary to locate CLP.
- **Takeaway:** SQL Variant can complement CLP; Iceberg/Parquet Variant remains an alternative physical layout.
- **Important points:**
  - Unshredded: row-local binary value without independent inner-field columns or statistics.
  - Shredded: selected paths gain columns, compression, and skipping; later changes require rewrites.
  - Best fit: interoperable tables with predictable hot paths.

## 2. What the connector preserves from CLP

- **Goal:** Introduce only the CLP concepts needed for comparison.
- **Why:** The [August 2025 YScope post](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) owns the complete CLP and optimizer walkthroughs.
- **Takeaway:** CLP intrinsically columnizes data and applies log-aware encoding.
- **Important points:**
  - ERTs group recurring shapes; the archive schema tree maps fields and types to columns.
  - Logtypes separate recurring text from variables for compressed free-text search.
  - Fit: uncontrolled schemas and cross-field/text search; stable or unique shapes reduce the advantage.
  - Earlier conventional-Parquet benchmarks say nothing about Variant.

## 3. How predicates bind to storage

- **Goal:** Compare physical work after pushdown without repeating mechanics.
- **Why:** Logical predicate equivalence does not imply equivalent scans.
- **Takeaway:** CLP resolves paths per split; unshredded Variant traverses relevant row values.
- **Important points:**
  - Exact/polymorphic paths: shredded or `value` access versus homogeneous CLP columns.
  - Cross-field search: per-row Variant inspection versus CLP metadata candidate resolution.
  - Current CLP: typed wildcard value search. Future: wildcard keys and `{path, value}` projection.

## 4. Metadata at two scales

- **Goal:** Separate split planning from archive-local execution.
- **Why:** The earlier post covers split discovery, not the two-level comparison.
- **Takeaway:** Coarse metadata selects files; archive metadata selects encoded structures.
- **Important points:**
  - Uber extensions: billion-file planning and eligible Top-N early termination.
  - Future: structural summaries, key discovery, and semantic candidate narrowing.

## 5. Native execution becomes independently pluggable

- **Goal:** Explain the Presto Native extension boundary.
- **Why:** This is the article's main Presto contribution.
- **Takeaway:** Native execution moved out of the fork, not out of the worker.
- **Important points:**
  - Java: catalogs, splits, rewriting, serialization.
  - Native: load `.so`, deserialize handles, scan CLP, return batches.
  - PR #27653: dynamic factory registration; PR #27652: connector-specific native deserialization.
  - Result: no CLP-specific changes embedded in Presto or Velox.

## 6. Operator experience

- **Goal:** Show the shortest path to a working catalog.
- **Why:** Architecture needs one concrete usage example.
- **Takeaway:** Conventional Presto SQL over a storage-native scan.
- **Important points:** Install matched artifacts, enable loading, configure `clp.properties`, and show typed paths, wildcard search, and reconstruction.

## 7. Lessons for connector authors

- **Goal:** Provide a starting model for another external connector.
- **Why:** The architecture's community value extends beyond CLP.
- **Takeaway:** Ship a versioned Java/native contract and test the production boundary.
- **Important points:**
  - Define splits, pushdowns, residuals, and serialized handles first.
  - Package JAR, `.so`, dependencies, RPATH, and version pins together.
  - Match libc, architecture, CPU flags, and Folly `F14LinkCheck` mode.
  - Use a real native worker for plugin-bound integration tests.

## 8. One Presto, different storage contracts

- **Goal:** Close with evidence and a decision rule.
- **Why:** Readers need a practical choice, not a universal winner.
- **Takeaway:** One SQL engine can preserve different storage strengths without embedding every engine in its codebase.
- **Important points:**
  - Current OSS: native scans, typed paths, wildcard search, reconstruction, and pushdown.
  - Uber: about 25 PB/month raw to 750–850 TB; five 48-core workers; advanced planning remains Uber-specific.
  - Variant for interoperability; CLP for intrinsic columnization, cross-field/text search, compression, and lower transfer.

## Acknowledgments

- YScope: connector implementation.
- IBM and Presto community: infrastructure and review.
- YScope and Uber: production extensions and application.
