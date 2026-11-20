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

* **Language:** Go (Golang)
* **API & Transport:** HTTP (`net/http` or `chi` / `gin`) and/or gRPC
* **Message Broker:** Apache Kafka *or* RabbitMQ
* **Database:** PostgreSQL
* **Testing:** `testcontainers-go` (for spin-up integration tests with real dependencies)
* **Observability (Optional / Planned):** Structured logging (`slog`), Prometheus metrics, and OpenTelemetry tracing