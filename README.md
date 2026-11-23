# Reliable Payment & Settlement Engine

A backend service written in Go that processes online payments, handles temporary failures and duplicate requests safely, keeps a clear record of money movement, and checks for mismatches across transactions.

---

## What It Does (Features)

### 1. Safe Payment Creation & Lookup
* **Create Payments:** Accepts payment requests (such as amount, currency, customer, and merchant) through an API.
* **Duplicate-Safe Requests:** Supports an idempotency key so if a user or client retries a request due to a slow or dropped network connection, the system will not charge them twice. If the same key is reused with different payment details, the request is rejected.
* **Check Payment Status:** Lets callers look up a payment by ID to see its current stage, amount, timestamps, and any failure reason.

### 2. Payment Lifecycle Actions (Hold, Collect, Refund)
* **Step-by-Step Payment Stages:** Moves payments through clear stages—such as pending, approved/held, collected (captured), settled, refunded, or failed—and blocks invalid actions (for example, refunding a payment that was never collected).
* **Hold Now, Collect Later:** Separates checking/holding funds from actually collecting the money (helpful when a store or service only collects payment once an order is ready or fulfilled).
* **Full & Partial Refunds:** Supports returning all or part of a collected payment back to the customer.

### 3. Background Processing & Smart Retries
* **Async Task Handling:** Uses a message queue (Kafka or RabbitMQ) so payment steps can run in the background without making API callers wait on slow external payment processors.
* **Automatic Retries with Backoff:** Automatically retries temporary hiccups (like timeouts or brief processor outages) using gradually increasing wait times with slight randomness so retries don't overwhelm the system.
* **Immediate Failure on Permanent Errors:** Stops retrying right away when a payment is permanently declined (such as insufficient funds or an invalid payment method).

### 4. Dead Letter Queue (Failed Message Safety Net) & Replay
* **Isolating Bad or Stuck Messages:** Moves messages that keep failing or cannot be read to a separate Dead Letter Queue (DLQ) along with error details so they don't block healthy traffic.
* **Inspect & Replay:** Provides admin endpoints to view failed messages and re-run ("replay") them once an underlying issue is fixed.

### 5. Balanced Money Tracking (Double-Entry Ledger)
* **Complete Money Trail:** Keeps an append-only accounting log (a ledger) of every payment collection, fee, and refund—recording where money comes from and where it goes rather than just storing a single status.
* **Balanced Entries Every Time:** Every recorded movement balances out (money coming in/held equals money allocated to payouts, fees, or refunds), and past entries are never silently overwritten—adjustments are added as new entries.
* **Merchant Payout Summaries:** Summarizes collected amounts, fees, and net payouts for a merchant over a given time period.

### 6. Automated Mismatch Checks & Service Health
* **Reconciliation Checks:** Periodically scans payments and money records to spot discrepancies (such as a payment marked collected without matching accounting entries, or a payment stuck in an intermediate state) and lets operators track or resolve them.
* **Health & Readiness Checks:** Exposes health endpoints so monitoring tools or container platforms can verify the service is running and connected to its database and message queue.

---

## Tech Stack

* **Language:** Go 1.26
* **API & Transport:** HTTP/JSON with [chi](https://github.com/go-chi/chi) (no gRPC)
* **Message Broker:** Apache Kafka
* **Database:** PostgreSQL (`pgx/v5`, embedded SQL migrations)
* **Observability:** `slog` JSON logs, Prometheus metrics, OpenTelemetry tracing
* **Testing:** `testcontainers-go` (real Postgres and Kafka; no mocks of either)

Money is **INR only**, stored as integer **paise** (`amount_paise`, max 1e12). The platform fee is 2% by default (`DEFAULT_FEE_BPS=200`), overridable per merchant, rounded up at capture and frozen on the payment.

---

## Running it

Prerequisites: Go 1.26, Docker.

```sh
cp .env.example .env            # then export its variables (or use your shell's dotenv)
docker compose up -d --wait     # Postgres :5432 and Kafka :9092
go run ./cmd/api                # HTTP API on :8080
go run ./cmd/worker             # background worker; health + metrics on :8081
```

Migrations apply automatically at startup (`AUTO_MIGRATE=true`). The API and the worker are separate processes; run as many replicas of each as you like.

```sh
# create, authorize, capture
KEY=$(uuidgen)
PID=$(curl -s localhost:8080/v1/payments -H "Idempotency-Key: $KEY" \
      -d '{"merchant_id":"m1","customer_id":"c1","amount_paise":100000}' | jq -r .id)
curl -s -XPOST localhost:8080/v1/payments/$PID/authorize        # 202 + an operation to poll
curl -s localhost:8080/v1/payments/$PID                         # AUTHORIZED once the worker ran
curl -s -XPOST localhost:8080/v1/payments/$PID/capture
```

The mock processor is chosen by customer ID prefix: `decline_` declines authorize, `capture_decline_` declines capture, `refund_decline_` declines refund, and `timeout_` gives a transient error on every operation (useful for seeing retries and the DLQ).

### Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | *(required)* | Postgres connection string |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated brokers |
| `ADMIN_API_KEY` | *(required by the API)* | Bearer token for `/admin/*` and the API's `/metrics` |
| `HTTP_ADDR` / `WORKER_HTTP_ADDR` | `:8080` / `:8081` | API listen address / worker health+metrics address |
| `AUTO_MIGRATE` | `true` | Apply migrations at startup |
| `DEFAULT_FEE_BPS` | `200` | Platform fee in basis points |
| `COMMANDS_TOPIC`, `CONSUMER_GROUP`, `DLQ_CONSUMER_GROUP`, `TOPIC_PARTITIONS` | `payments.commands`, `settlement-worker`, `settlement-dlq-store`, `3` | Kafka layout |
| `MAX_ATTEMPTS`, `RETRY_BASE_DELAY`, `RETRY_MAX_DELAY` | `5`, `1s`, `1m` | Retry policy (exponential, ±20% jitter) |
| `OUTBOX_POLL_INTERVAL` | `200ms` | Relay polling when idle |
| `SETTLEMENT_DELAY`, `SETTLEMENT_INTERVAL` | `24h`, `1m` | Pay out a payment this long after capture; how often to look |
| `RECON_INTERVAL` | `5m` | Reconciliation period |
| `RECON_PENDING_PAYMENT_AFTER`, `RECON_AUTHORIZED_PAYMENT_AFTER` | `1h`, `168h` | When PENDING / AUTHORIZED counts as stuck |
| `RECON_OUTBOX_STALE_AFTER`, `RECON_OPERATION_STUCK_AFTER` | `5m`, `15m` | Plumbing lag thresholds |
| `RETENTION_OUTBOX`, `RETENTION_DEAD_LETTERS`, `RETENTION_INTERVAL` | `168h`, `720h`, `1h` | Purge published outbox rows / closed dead letters older than this |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | *(unset)* | Where to send traces (OTLP/HTTP). Unset: spans propagate and appear in logs, nothing is exported. Standard `OTEL_*` variables apply. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

---

## API

Errors are `{"error": {"code": "...", "message": "..."}}`. Merchant endpoints currently have **no authentication** (a known gap); `/admin/*` needs `Authorization: Bearer $ADMIN_API_KEY`.

### Payments

| Method & path | Notes |
|---|---|
| `POST /v1/payments` | Body `{merchant_id, customer_id, amount_paise, currency?}`. Requires `Idempotency-Key`. `201` new, `200` + `Idempotent-Replayed: true` for a repeat, `422` if the key is reused with different details. |
| `GET /v1/payments/{id}` | Status, amounts, fee, refunded, `captured_at`, `settled_at`. |
| `POST /v1/payments/{id}/authorize` · `/capture` · `/void` | Validated cheaply, then queued: `202` with an operation (`Location: /v1/operations/{id}`). |
| `POST /v1/payments/{id}/refund` | Body `{amount_paise?}` (omit to refund the remainder). Requires `Idempotency-Key` (scoped to the payment): a repeat returns the original operation (`200`). |
| `GET /v1/operations/{id}` | Poll: `PENDING` → `SUCCEEDED` / `FAILED` (with `error`) / `DEAD_LETTERED`. |
| `GET /v1/merchants/{id}/payouts/summary?from=&to=` | RFC 3339 window `[from, to)`: gross collected, refunded, fees, net earned, and `settled_paise` actually paid out. |

Lifecycle: `PENDING → AUTHORIZED → CAPTURED → SETTLED`, with `PARTIALLY_REFUNDED`/`REFUNDED` after captures and `VOIDED`/`FAILED` as dead ends. Partial capture is not supported.

### Admin (`/admin`, bearer token)

| Path | Purpose |
|---|---|
| `GET/PUT /admin/merchants/{id}/fee` | Per-merchant fee override `{fee_bps}` |
| `GET /admin/dlq`, `GET /admin/dlq/{id}` | List (`status`, `limit`, `cursor`) / inspect dead letters |
| `POST /admin/dlq/{id}/replay`, `/dismiss` | Re-run a dead-lettered operation once the fault is fixed / close it with a note |
| `GET /admin/reconciliation/discrepancies[/{id}]` | Findings (`status`, `type`, `limit`, `cursor`) |
| `POST /admin/reconciliation/discrepancies/{id}/acknowledge` | `{note?}` — seen, being looked at |
| `POST /admin/reconciliation/discrepancies/{id}/resolve` | `{note}` (required) — handled outside the system |
| `POST /admin/reconciliation/discrepancies/{id}/correct` | `{note, entries?}` — fix a ledger finding with a new ledger transaction (see below) |
| `POST /admin/reconciliation/run` | Run the checks now (`409` if a run is in progress) |

### Health and metrics

| Process | Endpoint | |
|---|---|---|
| API | `GET /healthz`, `GET /readyz` | Readiness checks Postgres and Kafka |
| API | `GET /metrics` | Prometheus; **requires the admin bearer token** (`authorization:` in the scrape config) |
| Worker | `GET /healthz`, `GET /readyz`, `GET /metrics` on `WORKER_HTTP_ADDR` | Readiness also requires the relay, settlement, reconciliation and retention loops to have run recently. **Unauthenticated: keep it on a private network.** |

---

## Architecture

```
            ┌────────┐  202   ┌──────────────────────────────┐
  client ──▶│  API   │───────▶│ Postgres                     │
            └────────┘        │  operations + outbox (1 tx)  │◀───────────┐
                              └───────┬──────────────────────┘            │
                                      │ FOR UPDATE SKIP LOCKED            │ outcome (fenced UPDATE),
                              ┌───────▼───────┐   payments.commands       │ retry = new outbox row
                              │  outbox relay │──────────────▶ Kafka ─────┤
                              └───────────────┘                │          │
                                                       ┌───────▼────────┐ │
                                                       │ command worker │─┘
                                                       │ payment.Service│── ledger (same tx)
                                                       └───────┬────────┘
                                         exhausted retries     │ payments.commands.dlq
                                                       ┌───────▼────────┐
                                                       │ DLQ store      │─▶ dead_letters ─▶ /admin/dlq
                                                       └────────────────┘
  worker also runs: settlement · reconciliation · retention · health/metrics
```

**Request path.** The API validates cheaply, then writes an `operations` row and an `outbox` row in one transaction and returns `202`. Every state change that emits work goes through the outbox, so a committed change always has its message and vice versa.

**Delivery is at-least-once, and every handler is idempotent.** The relay publishes outbox rows to Kafka (keyed by payment ID, so one payment's operations stay ordered) and can run in several replicas. The worker runs `payment.Service`, which locks the payment row `FOR UPDATE`, calls the processor, then saves. Each message carries an attempt number and outcomes are written with a fenced `UPDATE … WHERE status='PENDING' AND attempts < N`, so duplicate or stale deliveries change nothing.

**Failures.** Permanent errors (declines, invalid state) end the operation as `FAILED`. Transient errors schedule a retry as a new outbox row with a delay computed on the **database clock** (never the app clock), exponential with ±20% jitter. Exhausted retries end as `DEAD_LETTERED` and publish to the DLQ topic; operators replay or dismiss from `/admin/dlq`. Replaying an unreadable (poison) message just returns it to the DLQ, by design.

**Ledger.** Append-only double entry: `ledger_accounts`, `ledger_transactions` (kinds `CAPTURE`, `REFUND`, `SETTLEMENT`, `ADJUSTMENT`) and `ledger_entries`. Database triggers reject `UPDATE`/`DELETE`, and a deferred constraint trigger enforces debits = credits at commit. `UNIQUE (kind, ref_id)` makes each event postable once. Accounts: `CUSTOMER_FUNDS`, `PLATFORM_FEES`, `MERCHANT_PAYABLE:<m>`, `MERCHANT_PAID_OUT:<m>`.

```
capture ₹1,000 @2%      DEBIT CUSTOMER_FUNDS 100000 | CREDIT MERCHANT_PAYABLE 98000, PLATFORM_FEES 2000
partial refund          DEBIT MERCHANT_PAYABLE, PLATFORM_FEES (fee shares are cumulative) | CREDIT CUSTOMER_FUNDS
settlement (payout)     DEBIT MERCHANT_PAYABLE <owed> | CREDIT MERCHANT_PAID_OUT <owed>
```

**Settlement.** A worker loop pays out payments `SETTLEMENT_DELAY` after capture: one `SETTLEMENT` ledger transaction for what is currently owed on the payment, `settled_at` set, and status `CAPTURED → SETTLED` (a `PARTIALLY_REFUNDED` payment is paid out too but keeps its status). A refund after payout debits the merchant's payable, which can go negative: that is a clawback against the next payout. Fully refunded payments never settle.

**Reconciliation.** Every `RECON_INTERVAL` the worker runs ten checks in one read-only `REPEATABLE READ` snapshot and records findings in `discrepancies` (deduplicated per `(type, subject)`; a finding whose condition clears resolves itself; a resolved problem that returns opens a new finding):
captured payment without a CAPTURE ledger transaction · CUSTOMER_FUNDS net ≠ captured − refunded · unbalanced ledger transaction · payment stuck PENDING/AUTHORIZED · refunds exceed capture · ledger activity on a payment that never collected money · stale outbox row · stuck or dead-lettered operation · global debits ≠ credits per currency · settlement state disagrees with the ledger.
The reconciler never changes money. To fix a ledger finding, an operator calls `/correct`: for a missing capture the server re-posts the normal CAPTURE from the payment's frozen amount and fee; for a funds mismatch or stray ledger activity the operator supplies entries, posted as an `ADJUSTMENT` whose `ref_id` is the finding (so a finding is corrected once). It runs in one transaction with the finding and payment locked, and rolls back if the ledger would still disagree with the payment afterwards. Everything else (stuck work, refund overruns) is acknowledged or resolved with a note.

**Retention.** The worker deletes published outbox rows older than `RETENTION_OUTBOX` and replayed/dismissed dead letters older than `RETENTION_DEAD_LETTERS`, in batches. Pending dead letters, and dead letters of an operation still `DEAD_LETTERED`, are never deleted.

---

## Observability

**Metrics** (`settlement_` prefix): `http_requests_total` and `http_request_duration_seconds` (labelled by route pattern, never raw path); `operations_finished_total{kind,outcome}`, `operation_retries_total`, `operation_duration_seconds`; `outbox_published_total`, `outbox_publish_errors_total`; `settlements_total`; `reconciliation_runs_total`, `reconciliation_new_findings_total`, `reconciliation_last_success_timestamp_seconds`; `retention_purged_total`; `kafka_handler_errors_total`, `dead_letters_recorded_total`. The worker also exports gauges read from the database at scrape time: `outbox_pending_messages`, `outbox_oldest_pending_age_seconds` (relay lag), `outbox_scheduled_messages`, `dead_letters_pending`, `operations_unfinished{status}`, `reconciliation_open_findings{type,status}`.

Useful alerts: outbox lag above a few seconds; `dead_letters_pending > 0`; `reconciliation_open_findings{type!="PAYMENT_STUCK"} > 0`; `time() - reconciliation_last_success_timestamp_seconds > 3 * RECON_INTERVAL`; a rising `operations_finished_total{outcome="dead_lettered"}`.

**Tracing.** A request's trace continues through the outbox row (the W3C `traceparent` is stored in the row's headers), the relay's publish, the Kafka message, the consumer and the worker's operation span; retries and DLQ hops stay in the same trace. Request logs carry `trace_id`. Database calls are not separately traced.

---

## Testing

```sh
go vet ./...
go test ./... -race -count=1     # ~2 minutes; needs Docker
```

Tests use real Postgres and Kafka through testcontainers: a test that cannot reach Docker **skips**, so check the output for `SKIP` before trusting a green run. They include concurrency tests (duplicate deliveries, simultaneous refunds, captures, settlements, corrections, purges) and `internal/chaos`, which runs full payment lifecycles while killing database connections, restarting workers, replaying duplicate and poison messages and failing the processor, then checks that every operation finished, the outbox drained, the ledger balances and the reconciler finds nothing wrong (`go test ./internal/chaos -run Chaos -v`; skipped by `-short`).

---

## Known gaps

* **Merchant endpoints are unauthenticated**, including the payout summary.
* **Partial capture** is unsupported: capture takes the full amount.
* **Processor idempotency.** The processor is called while the payment row is locked and the result is saved afterwards. A crash between the two makes the next attempt call the processor again for the same `ReferenceID`; a real processor must deduplicate on it. (The chaos test logs how often this happens.)
* **Worker `/metrics` and health are unauthenticated** by design; they must not be exposed publicly.
* Reconciliation checks are full scans (fine at modest volume; add a lookback window before the tables get large).
