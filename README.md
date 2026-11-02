# recallbeam

Rebuildable fleet search over [handoffkeep](https://github.com/mgh3326/handoffkeep).
The source keeps the originals; recallbeam owns only its Oracle Text index.
HTTP and a read-only MCP tool share query validation and ranking.

## Build and run

Requires Go 1.27. Builds use the pure-Go `github.com/sijms/go-ora/v2`
driver and need no Oracle client libraries or CGO.

```sh
CGO_ENABLED=0 go build ./cmd/recallbeam
recallbeam migrate
recallbeam serve
```

Run migrations explicitly against the intended database before starting the
service. `migrate` applies the embedded, versioned SQL in `migrations/` and
records completed versions in `rb_migrations`. Oracle DDL commits implicitly:
a partial migration needs inspection and repair before retrying. Run only one
migration command at a time. The database account needs schema DDL and Oracle
Text privileges; runtime needs DML on these tables and access to Oracle Text.

## Configuration

Supply secrets through the process environment. Do not check environment files,
connection details, or wallets into source control. `wallet/` is ignored.

| Environment variable | Purpose |
| --- | --- |
| `RB_DB_HOST` | Database endpoint, required |
| `RB_DB_PORT` | TLS listener port, default 1522 |
| `RB_DB_SERVICE` | Database service, required |
| `RB_DB_USER` | Database username, required |
| `RB_DB_PASSWORD` | Database password, required |
| `RB_WALLET_DIR` | Wallet directory containing the auto-login wallet, required |
| `RB_HK_URL` | Source base URL; required for serve and rebuild |
| `RB_HK_TOKEN` | Source bearer token with read scope; required for serve and rebuild |
| `RB_SEARCH_TOKEN` | Search bearer token; required for serve |
| `RB_LISTEN_ADDR` | HTTP bind address, default port 8080 |
| `RB_SYNC_INTERVAL` | Poll interval as a Go duration; default 15s, range 1s–1h |

The driver enables TLS with certificate verification. Keep the wallet readable
only by the service account. Logs omit connection strings and upstream bodies.

## Search

`GET /v1/search?q=&model=&limit=` requires `Authorization: Bearer` with
`RB_SEARCH_TOKEN`. `q` is 1–256 characters; `model` optionally selects
`document`, `task`, `task_comment`, or `decision`; `limit` defaults to 20 and
must be 1–100. The response has a `results` array containing source, model,
model ID, key, identifier, title, body, tags, update time, source sync ID,
ranking tier, and text score. Results use stable source/model/ID tie-breaks.

1. Exact identifier, document key, or model ID, ignoring case. Source documents
   under `linear/` also expose the issue identifier; task titles beginning with
   an issue identifier expose it too.
2. Key or title prefix, shorter matching key/title first.
3. Oracle Text score, descending.

The query is literal text, not an Oracle Text expression. Whitespace-separated
terms are joined with AND; SQL LIKE metacharacters are escaped. Invalid queries
return 400, missing or invalid auth returns 401, and store failures return 503.
The endpoint never logs queries or upstream credentials.

`recallbeam mcp` serves newline-delimited MCP JSON-RPC over stdio. It implements
initialize, ping, tools/list, and tools/call, exposing just `search` with the
same `q`, `model`, and `limit` arguments. Configure the executable and database
environment in the MCP client. Stdio is a local process boundary; HTTP uses the
search bearer token. Run `serve` separately to maintain ingestion. Protocol
versions 2024-11-05, 2025-03-26, and 2025-06-18 are supported.

## Ingestion and rebuild

The source adapter consumes read-authenticated `GET /v1/sync/bootstrap` and
`GET /v1/sync/delta` with pages of at most 1000. Bootstrap JSON contains `rows`
(with string `id`, `updated_at`, and document `key`), opaque `cursor`,
`max_sync_id`, and `schema_version`. Delta is NDJSON with a required final
`_metadata_` object. Truncated, unordered, unknown-model, and non-progressing
feeds fail without advancing the committed cursor.

Current rows are fetched from `/v1/documents/{key}`, `/v1/tasks/{id}`,
and `/v1/tasks/{id}/comments`. Document IDs without a known key are resolved
by walking every document bootstrap page. Comments and decisions are resolved
by walking every task bootstrap page: comment lookup probes each task's
`after_id` cursor, while decision lookup matches `refs.decision_request.id`.
These complete scans work with the existing read APIs; they do not truncate
at a list limit or silently treat an unavailable endpoint as deletion.
Task refs and event notes, and decision questions and resolutions, are indexed.
A racing row 404 removes stale indexed content; other errors preserve the
cursor and retry next poll.

Global-ID reads would make this much cheaper: v0 may need O(number of tasks)
requests per comment or decision, and O(number of documents) identity scanning
per document delta. Monitor sync lag on large sources. A future source lookup
endpoint can replace the resolver without changing the engine or store.

Each delta page's mutations and cursor commit together. A database row lock
serializes writers across processes, including manual rebuilds. Search keeps
running while a writer bootstraps or replays changes.

With no cursor, on HTTP 410, or on schema version change, the engine rebuilds
an inactive table set. It takes the earliest bootstrap watermark across all
models, then replays delta to the high-water mark observed at replay start.
The final delta transaction publishes the inactive set by changing
`rb_state.active_slot` alongside its cursor. A failed rebuild keeps the old
index active; retry clears the abandoned inactive set. The next rebuild or
reconciliation reclaims old rows, so allow room for two copies of the index.

`rb_docs` and `rb_cursor` are active-set views over paired physical tables
`rb_docs_a/b` and `rb_cursor_a/b`. Physical document primary keys are
`(source, model, model_id)`. Search reads both branches and the pointer in one
SQL statement, using Oracle statement-level read consistency across a swap.
No DDL runs during a rebuild.

`recallbeam serve` polls continuously and performs a full reconciliation every
24 hours. `recallbeam rebuild` forces a rebuild immediately. Failures leave the
last published index available. A schema change during rebuilding aborts it;
the next poll retries. Source retention must exceed the time needed for a full
bootstrap and replay. Monitor sync errors and compare the active cursor with
the source high-water mark. Oracle Text failures can be investigated through
`CTX_USER_INDEX_ERRORS` and pending work through `CTX_USER_PENDING`.

## Oracle Text and operations

Both table sets index `search_text`, a CLOB containing title plus body. Every
upsert updates it, including title-only changes. The `AUTO_LEXER` preference
supports mixed Korean/English content without separate language columns, and
an empty stoplist retains short words. `SYNC (ON COMMIT)` makes committed
changes searchable before publishing a rebuilt set. See the
[Oracle Text reference](https://docs.oracle.com/en/database/oracle/oracle-database/21/ccref/text-reference.pdf)
and [driver documentation](https://github.com/sijms/go-ora).

Korean relevance, stemming, commit latency, wallet connectivity, and ADB's
Oracle Text configuration need a deployment smoke test; fake unit tests cannot
establish those properties. The desktop test workflow never contacts a live
source or database. No search log is retained in v0.

Long-lived serve and MCP processes issue `SELECT 1 FROM dual` every 12 hours,
independently of ingestion, to keep an otherwise idle database active. Failures
are logged and retried on the next interval. Run the service continuously under
a process supervisor; a stopped process cannot prevent database auto-stop.
SIGINT/SIGTERM cancels ingestion and shuts down the HTTP listener.

## Verification

```sh
go vet ./...
go test -p 2 ./...
```

Tests use an HTTP fake source, an in-memory transactional index, and SQL mocks.
No local database server is needed. CI also runs the race detector and builds
with CGO disabled.

## License

Apache-2.0
