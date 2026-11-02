# recallbeam

Search service that finds a fleet's documents, tasks and decisions again.

recallbeam owns a rebuildable search index. It never holds the original data:
a source of record (first [handoffkeep](https://github.com/mgh3326/handoffkeep))
keeps every document, and recallbeam pulls its change feed, indexes it, and
answers search requests. If recallbeam is down, the source falls back to its
own search; if the index is lost, recallbeam rebuilds it from the source.

- **Ingest:** pulls each source's change feed by cursor, upserts idempotently,
  and reconciles the full set daily.
- **Search:** an HTTP API and an MCP tool for coding agents, with Korean and
  English full-text ranking.
- **Store:** an Oracle Autonomous Database with Oracle Text, owned only by this
  service. Other stores can follow behind the same interface.

Status: v0 under construction.

## License

Apache-2.0
