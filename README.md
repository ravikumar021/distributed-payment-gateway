# Payment reliability simulator

A runnable Go API and background worker demonstrating durable payment state, merchant-scoped idempotency, bounded retries and an atomic event outbox. The provider is simulated: **no money moves and no card data is accepted**.

## Quick start

Requires Go 1.24+ (tested with 1.27.4). SQLite is embedded; no database server is needed.

```sh
export PAYMENT_API_KEYS='{"local-demo-secret-change-me":"merchant-a"}'
go run ./cmd/api
# In another terminal from the same directory:
go run ./cmd/worker
```

Create a payment:

```sh
curl -i http://127.0.0.1:8080/payments \
  -H 'Authorization: Bearer local-demo-secret-change-me' \
  -H 'Idempotency-Key: order-001' \
  -H 'Content-Type: application/json' \
  -d '{"amount_minor":1299,"currency":"EUR","scenario":"transient"}'
```

Repeat the exact request: the same payment ID is returned and `Idempotent-Replay: true` is set. Change the amount with the same key: HTTP 409. Amounts are integer minor units; 1299 EUR means EUR 12.99. Supported demo currencies: INR, EUR, USD, GBP, CAD, AUD, NZD.

Read the payment using the returned ID:

```sh
curl http://127.0.0.1:8080/payments/PAYMENT_ID \
  -H 'Authorization: Bearer local-demo-secret-change-me'
```

`POST /payments` always returns 202 for a valid new request or replay; inspect `status` to distinguish pending and terminal results. A different merchant cannot read the payment. `GET /health` is unauthenticated liveness only.

## Provider scenarios

| Scenario | Behavior |
|---|---|
| `success` (default) | Succeeds on first attempt |
| `decline` | Fails permanently on first attempt |
| `transient` | Two failures then success; retries after 1 and 2 seconds |
| `unavailable` | Fails permanently after three attempts |

Payment state is `pending`, `succeeded`, or `failed`. Terminal payments are not processed again. Retry times and attempts survive restarts.

## Reliability design

```text
Authenticated request -> merchant + idempotency key -> SQLite transaction
                                                  -> payment + outbox event
Worker -> due pending payment -> simulated result -> state + outbox transaction
Outbox -> JSON lines on stdout -> delivery marker
```

- Request fingerprints use normalized currency, integer amount and scenario. Reusing a key with a different request returns a conflict. Keys are scoped to the authenticated merchant and retained indefinitely in this demo.
- SQLite immediate transactions and a unique merchant/key constraint serialize concurrent writers. Tests use multiple store connections and simultaneous requests.
- Payment changes and their outbox events commit together. An injected outbox failure verifies rollback.
- Outbox delivery is **at least once**, not exactly once: a crash after writing to the sink but before recording delivery can produce duplicates. Multiple dispatchers can also emit duplicates. Consumers must deduplicate `event_id`; prefer one dispatcher for this demo.
- API and worker are separate processes sharing the same SQLite file on one host. This is not a multi-host distributed database or a Kafka implementation.
- Provider simulation occurs inside the transaction. A real provider adapter needs a lease/claim mechanism, provider-side idempotency, timeouts and reconciliation; do not insert network calls into that transaction.

## Containers

```sh
export PAYMENT_API_KEYS='{"local-demo-secret-change-me":"merchant-a"}'
docker compose up --build
```

Both services share a named volume. The API port is bound to localhost. Use a real randomly generated token for any nontrivial local evaluation; never commit secrets. Container images are built by CI. `PAYMENT_DB` selects the database file; `PAYMENT_ADDR` defaults to `127.0.0.1:8080` outside Docker.

## Validation

```sh
go test -race ./...
go vet ./...
go build ./...
```

Tests cover conflicting/repeated requests, concurrent writers, merchant isolation, invalid amounts, transaction rollback, retry timing/exhaustion, database reopen, outbox sink failure and HTTP validation. No throughput claim or production reliability claim is made.

## Boundaries

This is a complete local demonstration, not a production gateway. It does not implement real payment rails, PCI compliance, refunds, settlement, webhooks, TLS termination, key rotation, rate limiting, telemetry export or high availability. The worker requires trusted database access. SQLite storage and stdout delivery target small local workloads; database backup and retention policies remain operator responsibilities.

Prepared with AI coding assistance. Commit dates reflect when the work was performed. It does not represent past employer production code or historical project results.
