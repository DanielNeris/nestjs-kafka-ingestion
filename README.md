# NestJS Kafka Course — Challenge Repo

A NestJS monorepo with a **Wikimedia producer** (SSE → Kafka) and an **OpenSearch consumer** (Kafka → OpenSearch), designed for **at-least-once** delivery and **idempotent** indexing. This document explains the architecture, design choices, and how to run the project so an evaluator can follow the implementation.

---

## Overview

The system ingests [Wikimedia recent changes](https://stream.wikimedia.org/v2/stream/recentchange) via Server-Sent Events, publishes them to Kafka, and indexes them in OpenSearch. The design prioritizes:

- **At-least-once semantics** — Kafka consumer commits only after successful processing.
- **Idempotent indexing** — Re-processing the same event overwrites the same document in OpenSearch (no duplicates).
- **Separation of concerns** — Shared libraries for config, contracts, Kafka, and OpenSearch; apps stay thin.

---

## What’s in the Repo

| Component                            | Role                                                                                                                                     |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------- |
| **wikimedia-producer-microservice**  | Connects to the Wikimedia SSE stream, wraps each event in an `EventEnvelope`, and publishes to the Kafka topic `wikimedia.recentchange`. |
| **opensearch-consumer-microservice** | Consumes `wikimedia.recentchange`, deduplicates by `payload.meta.id`, and bulk-indexes into OpenSearch in the `wikimedia` index.         |
| **libs/config**                      | Environment validation with Zod; single source of truth for `KAFKA_BROKERS`, `OPENSEARCH_NODE`, `PORT`, etc.                             |
| **libs/contracts**                   | Topic names and the `EventEnvelope` shape so producer and consumer agree on the contract.                                                |
| **libs/kafka**                       | Kafka producer and a bootstrap consumer module (NestJS integration).                                                                     |
| **libs/opensearch**                  | OpenSearch client, index creation (`ensureIndex`), and bulk indexing helpers.                                                            |

All apps and libs use path aliases (`@app/config`, `@app/contracts`, etc.) so imports stay clean and the monorepo structure is explicit.

---

## Design Decisions (for the evaluator)

1. **Event envelope** — Every message is wrapped in `EventEnvelope` with `payload` (raw event) and `meta` (e.g. `id`, `source`). This keeps a stable document id for OpenSearch (`payload.meta.id`) and makes it easy to add metadata later.
2. **Manual commit in the consumer** — `autoCommit: false`; the Nest handler commits only after the message is processed. That gives at-least-once: we may see redeliveries, but we never skip messages.
3. **OpenSearch document id = event id** — We use `payload.meta.id` as `_id` in OpenSearch. Reprocessing the same event just overwrites the same doc, so the pipeline is safe under redelivery.
4. **Tests in a dedicated folder** — All specs live under `test/` (unit, functional, e2e) so production code stays free of test files. Unit tests mirror the monorepo layout and use the same path aliases.

---

## Prerequisites

- **Node 20+**
- **pnpm**
- **Docker & Docker Compose** (for Kafka, OpenSearch, and optionally the apps)

---

## How to Run

### 1. Install dependencies

```bash
pnpm install
```

### 2. Environment variables

Copy the example file and adjust if needed:

```bash
cp .env.example .env
```

Main variables (with defaults):

| Variable          | Description                         | Default                 |
| ----------------- | ----------------------------------- | ----------------------- |
| `KAFKA_BROKERS`   | Kafka broker list (comma-separated) | `localhost:9092`        |
| `OPENSEARCH_NODE` | OpenSearch URL                      | `http://localhost:9200` |
| `PORT`            | HTTP port (producer)                | `3000`                  |

When running **inside Docker**, the compose files set `KAFKA_BROKERS=kafka:19092` and `OPENSEARCH_NODE=http://opensearch:9200` for the services.

### 3. Run everything with Docker

```bash
# Infra + both microservices
docker compose up -d --build

# Or only infra (Kafka + OpenSearch) for local development
docker compose up -d kafka opensearch opensearch-dashboards
```

Useful endpoints:

| Service               | URL                                                       |
| --------------------- | --------------------------------------------------------- |
| Kafka                 | `localhost:9092` (external)                               |
| OpenSearch            | http://localhost:9200                                     |
| OpenSearch Dashboards | http://localhost:5601                                     |
| Conduktor (Kafka UI)  | http://localhost:8080 (if using full compose)             |
| Producer HTTP         | http://localhost:3001 — `GET /` starts the stream handler |

### 4. Run apps locally (no Docker for the apps)

With Kafka and OpenSearch already up (e.g. via Docker):

```bash
# Terminal 1 — producer (publishes to Kafka)
pnpm exec nest start wikimedia-producer-microservice --watch

# Terminal 2 — consumer (consumes Kafka, indexes to OpenSearch)
pnpm exec nest start opensearch-consumer-microservice --watch
```

Use `KAFKA_BROKERS=localhost:9092` and `OPENSEARCH_NODE=http://localhost:9200` in `.env` (or export them) so the apps talk to the containers.

### 5. Dev mode with Docker (mounted code + watch)

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build
```

Apps run inside Docker with the code mounted and `--watch` for live reload.

---

## Scripts

| Script                                                  | Description                                                                             |
| ------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `pnpm run build`                                        | Build default app (wikimedia-producer)                                                  |
| `pnpm exec nest build opensearch-consumer-microservice` | Build the consumer app                                                                  |
| `pnpm run test`                                         | Unit tests (Jest) — specs in `test/unit/`, descriptions use **should** / **should not** |
| `pnpm run test:cov`                                     | Unit tests with coverage report                                                         |
| `pnpm run test:functional`                              | Functional tests — flows with real modules, I/O mocked (see `test/functional/`)         |
| `pnpm run test:e2e`                                     | E2E tests (see `test/e2e/`)                                                             |
| `pnpm run test:all`                                     | Unit (with coverage) + functional + e2e                                                 |
| `pnpm run lint`                                         | ESLint                                                                                  |
| `docker compose up -d`                                  | Start stack                                                                             |
| `docker compose down`                                   | Stop stack                                                                              |

---

## Monorepo layout

```
apps/
  wikimedia-producer-microservice/   # SSE → Kafka
  opensearch-consumer-microservice/  # Kafka → OpenSearch
libs/
  config/      # Env validation (Zod)
  contracts/   # Topics, EventEnvelope
  kafka/       # Producer, bootstrap consumer
  opensearch/  # Client, ensureIndex, bulkIndex
test/
  unit/        # Unit tests (*.spec.ts) — all descriptions use "should" / "should not"
  functional/  # Functional tests (*.functional-spec.ts) — full flows, I/O mocked
  e2e/         # End-to-end tests
```

See `test/README.md` for how tests are organized and which aliases they use.

---

## Delivery semantics and idempotency

- **Kafka consumer:** `autoCommit: false`; the Nest app commits **after** the handler runs. So we get **at-least-once**: every message is processed at least once; redelivery can happen.
- **OpenSearch:** Each document is indexed with `_id = payload.meta.id`. Processing the same event again only overwrites that document. Indexing is **idempotent** and safe under at-least-once.

---

## License

UNLICENSED (private).
