This directory preserves the original Wikipedia-to-graph extraction pipeline that was built before the repo switched to `survivoR` as the primary structured data source.

The archived code downloads Survivor season pages, extracts and classifies Wikipedia tables, normalizes them with an LLM, and writes the resulting structured facts into Neo4j.

It was archived because the raw Wikipedia tables are inconsistent across seasons, the extraction path is harder to reproduce, and the `survivoR` dataset gives the repo a simpler and more reliable main setup path.

If you want to revisit this approach, the archived scripts, libraries, and tests still contain the original parsing and ingestion flow.
