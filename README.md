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
| `RB_HK_URL` | Source base URL; HTTPS required except HTTP to literal loopback IPs; required for serve and rebuild |
| `RB_HK_TOKEN` | Source bearer token with read scope; required for serve and rebuild |
| `RB_SEARCH_TOKEN` | Search bearer token; required for serve |
| `RB_LISTEN_ADDR` | HTTP bind address, default IPv4 loopback on port 8080; non-loopback overrides require a protected proxy network |
| `RB_SYNC_INTERVAL` | Poll interval as a Go duration; default 15s, range 1s–1h |

The driver enables TLS with certificate verification. Keep the wallet readable
only by the service account. Logs omit connection strings and upstream bodies.

## Search

For remote access, a TLS-terminating reverse proxy is required. The backend
serves plaintext HTTP and binds to IPv4 loopback by default. Keep it reachable
only by the proxy, and expose only the proxy's HTTPS listener to clients;
`RB_SEARCH_TOKEN` and returned content must never cross an untrusted plaintext
network. `RB_LISTEN_ADDR` can override the binding for a proxy in a separate
network namespace, provided firewall/network policy restricts backend access
to that proxy. The override does not enable TLS on the backend.

Source requests require HTTPS before a bearer token is attached. Plain HTTP
is permitted only for literal loopback IPs, allowing local tests and tunnels;
remote HTTP URLs, DNS hostnames over HTTP, and URL userinfo are rejected.
Redirects are not followed, including with an injected HTTP client.

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

Current rows are fetched one request per row. Documents, comments and
decisions use `GET /v1/sync/rows/{model}/{model_id}`; decision IDs keep their
slashes in the path. Tasks use `/v1/tasks/{id}`, because the raw task row
lacks the event notes that are indexed. Task refs and event notes, and the
whole decision request (question, options, resolution), are indexed. A 404
carrying hk's `deleted` or `not_found` marker removes the indexed row; any
other 404 means the endpoint is unavailable and never becomes a delete.

Within one delta page, events are coalesced per model and ID: the row's last
event decides between a delete and one fetch of its current content.

A fetch failure holds the cursor and retries next poll. When the same row
fails on `RowAttempts` (3) consecutive polls with a row-specific error (for
example a 500 or an undecodable or oversized body), it is logged as
`sync.row_skipped` and left out of the page, which then commits with its
cursor. The row keeps its previously indexed content until a later change or
the daily rebuild fetches it; a rebuild skips it the same way. Source-wide
failures (unreachable source, 401, 403, 429, 502, 503, 504, missing endpoint)
are never skipped and always hold the cursor. Attempt counts live in memory:
a restart grants a row fresh attempts, and `recallbeam rebuild` retries the
rebuild up to `RowAttempts` times in one run. A row whose first failures occur
during the daily rebuild delays that rebuild, not delta polling.

Each delta page's mutations and cursor commit together. A database row lock
serializes writers across processes, including manual rebuilds. Search keeps
running while a writer bootstraps or replays changes.

With no cursor, on HTTP 410, or when the stored generation differs from the
running build's, the engine rebuilds an inactive table set. The generation is
recallbeam's own key: a hash of the indexed models, `hk.ExtractVersion` (how
rows map to indexed fields), `oracle.TextVersion` (how `search_text` is built)
and every embedded migration (lexer, stoplist, index parameters). Bump the
matching version with any change to extraction or `search_text`;
`TestExtractionPinsVersion` fails until the extraction bump is made. The
generation is stored in the cursor tables' `schema_version` column, so the
first start after this change rebuilds once.

A rebuild takes the earliest bootstrap watermark across all models, then
replays delta to the high-water mark observed at replay start. The final
delta transaction publishes the inactive set by changing `rb_state.active_slot`
alongside its cursor. A failed rebuild keeps the old index active; retry
clears the abandoned inactive set. The next rebuild or reconciliation reclaims
old rows, so allow room for two copies of the index.

hk's `schema_version` is not part of the generation, because most hk
migrations do not touch the rows recallbeam reads. A change is logged as
`sync.source_schema_changed`. Every fetched row is checked for the fields the
extractor reads and their JSON type. A missing or retyped field returns
`ErrIncompatible`, logged as `sync.row_incompatible` on each poll. That error
holds the cursor and is never skipped, and the index stays as it was until a
recallbeam release handles the new shape.

`rb_docs` and `rb_cursor` are active-set views over paired physical tables
`rb_docs_a/b` and `rb_cursor_a/b`. Physical document primary keys are
`(source, model, model_id)`. Search reads both branches and the pointer in one
SQL statement, using Oracle statement-level read consistency across a swap.
No DDL runs during a rebuild.

`recallbeam serve` polls continuously and performs a full reconciliation every
24 hours. `recallbeam rebuild` forces a rebuild immediately. Failures leave the
last published index available. recallbeam does not abort a rebuild on a
source schema change. hk rejects a bootstrap cursor issued before its own
migration with 400, which fails that rebuild, and the next attempt retries.
Source retention must exceed the time needed for a full bootstrap and replay.
Monitor sync errors and compare the active cursor with the source high-water
mark. Oracle Text failures can be investigated through
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
MCP also exits on either signal while its client keeps stdin open.

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
