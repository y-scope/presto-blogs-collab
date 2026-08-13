# Social Media Copy

Replace `BLOG_URL` with the published Uber Engineering blog URL.

## LinkedIn

Uber's CLP workloads produce 25 PB of raw semi-structured logs each month. Yet five Presto Native workers serve SQL queries over the resulting compressed corpus. How?

In Part III of Uber's CLP series, we explain how the platform:

- Keeps arriving data compressed and queryable in KV-IR
- Consolidates older data into columnar CLP archives
- Uses metadata-driven planning and CLP-native search to avoid unnecessary work
- Exposes evolving, polymorphic data through typed SQL

Across a 30-day production measurement, these layers reduced the aggregate event count examined by more than 200,000× compared with time-range filtering alone.

The work brings together Uber, YScope, and the PrestoDB community—and establishes a foundation for future agent-oriented search and discovery.

Read the engineering story: [BLOG_URL](BLOG_URL)

#UberEngineering #PrestoDB #Observability

## X / Twitter

Uber's CLP workloads produce 25 PB of raw semi-structured logs each month. Five Presto Native workers serve SQL queries over the compressed corpus.

Part III explains how metadata planning and CLP-native scans avoid unnecessary work at every layer.

[BLOG_URL](BLOG_URL)
