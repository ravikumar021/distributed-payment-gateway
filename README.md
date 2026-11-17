# Distributed payment gateway scaffold

An early Go prototype for exploring payment reliability. The current API returns HTTP 202 at `POST /payment`; the worker only prints a startup message. Idempotency, Kafka consumption, Redis caching and PostgreSQL persistence are **not implemented**. No throughput benchmark has been run.

## Current gaps

The scaffold has no `go.mod`, reproducible build, complete container configuration or automated tests. The included Compose file is an unfinished sketch and should not be treated as a working deployment.

## Implementation plan

1. Define payment request validation and a stable idempotency-key contract.
2. Persist request fingerprints and payment state atomically, rejecting conflicting key reuse.
3. Write an outbox event in the same transaction; publish with retry and consumer deduplication.
4. Test concurrent duplicate requests, database rollback and worker crash recovery.
5. Measure a documented workload before making performance claims.

This repository is a learning prototype, not a production payment processor. Documentation cleanup was prepared with AI assistance; it removes claims that the current code does not substantiate.
