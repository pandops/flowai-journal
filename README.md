# journal

journal is FlowAI's read-side history. It consumes every event published
to the `ALL_EVENT` stream through its durable consumer `journal`, stores
each event once, and serves API Gateway the team-scoped reads behind the
operator views: task, log, control and Executor histories, the Executor
list, the dashboard calendar and the live team stream.

journal publishes no events and calls no other service. It owns only the
PostgreSQL schema `journal_schema`.

## Responsibilities

- Store every `ALL_EVENT` message once, keyed by `(publisher, publisher_seq)`
  (Executor events by `executor-<team>-<executor>`), with an `event_id` from
  one commit sequence. Stored events are never updated or deleted.
- Derive the team from the trusted source (the service's `team` header, or
  the team of an Executor's credentials), never from the payload.
- Attribute Executor events to the claim they name. Log chunks and lifecycle
  events that arrive before `claim_accepted` are held for up to 10 minutes,
  then accepted with a new commit-sequence value or dropped as
  `not_claim_holder`.
- Keep the Executor list from executor-registry's events: connection state,
  held claims, capacity and the latest reported IP. An Executor's own
  connection observations are stored and shown but never change its state.
- Serve the dashboard calendar for a validated week or month in the
  operator's IANA time zone.
- Stream a team's frames to API Gateway over a WebSocket, resumable by
  `after_frame_id`.

journal never gates claims, never creates its durable consumer (the
deployment does) and never reads another service's schema.

## Interfaces

Request/reply (NATS, `agents.md` rule 9 envelope), queue group `journal`,
inbox `_INBOX.journal`:

| Subject                          | REST view                 |
| -------------------------------- | ------------------------- |
| `journal.ui.task-events.sub`     | task event history        |
| `journal.ui.task-logs.sub`       | task log, by `log_offset` |
| `journal.ui.control-events.sub`  | one control's events      |
| `journal.ui.executor-events.sub` | one Executor's history    |
| `journal.ui.executor-states.sub` | the team's Executor list  |
| `journal.ui.dashboard.sub`       | week or month calendar    |

Pages carry an opaque HMAC cursor bound to the operation, team, operator,
filter and limit. Request and reply schemas live in `schemas/`; the durable
consumer is declared in `schemas/consumers.json`.

HTTP on `:8080`:

- `GET /v1/livez`, `GET /v1/readyz` (ready only with the broker connected,
  the durable consumer bound and the schema migrated).
- `GET /v1/teams/{team_id}/stream` for API Gateway only, with its bearer
  token and the verified `X-FlowAI-Team-ID`, `X-FlowAI-Operator-ID` and
  `X-FlowAI-Request-ID` headers. Frames follow `schemas/frame.schema.json`.

## Running

```
journal --config /etc/flowai/config.yaml
```

See `config.example.yaml`. The container reads NATS credentials from
`/run/flowai/nats/user.creds` and the stand CA from
`/run/flowai/stand-ca/ca.pem`. TLS is required for NATS (`tls://`) and
PostgreSQL (`sslmode: verify-full`).

Build the image from the repository root:

```
podman build -f svc/journal/Containerfile .
```

## Tests

```
npm test -- --service=journal --category=unit --category=integration
```

Unit tests use in-process fakes. Integration tests start their own
PostgreSQL container.
