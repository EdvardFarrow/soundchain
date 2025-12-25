# SoundChain (MVP)

## High-Load Royalty Billing & Analytics System (Spotify-like engine)

This project implements a real-time streaming data processing pipeline: from gRPC ingestion to real-time analytics. The primary focus is on performance (2.5k+ RPS on a local machine), fault tolerance, and observability.

### Tech Stack

* **Code:** Python 3.12, gRPC (Protobuf), uv
* **Message Broker:** Redpanda
* **Ingestion:** Python Worker (AsyncIO)
* **Storage:**
    * *ClickHouse:* Real-time analytics
    * *PostgreSQL:* Financial ledger (Double-Entry logic)
    * *Redis:* Idempotency and caching
* **Monitoring:** Prometheus + Grafana
* **Infrastructure:** Docker Compose

## Architecture

1. **Load Generator** → Generates traffic and hits the server.
2. **gRPC Service** → Accepts Protobuf messages, validates them, and pushes Raw Events to Redpanda.
3. **Worker (ETL)** →
    * Consumes batches from Redpanda
    * Parses and validates data
    * Inserts into ClickHouse (analytics)
    * Exposes metrics to Prometheus
4. **Grafana** → Visualizes business metrics and worker health 

## Quick Start

Requires Docker and uv.

### 1. Start 

```bash
# Build and start containers (DBs, Broker, Worker, Monitoring)
docker compose up -d --build
```

### 2. Run gRPC Server

In a separate terminal:
```Bash
make grpc-run
# Expected output: "Server started on [::]:50051"
```

### 3. Apply Load

In another terminal:
```Bash
make grpc-load
# The script will send a batch of requests (default 50k)
```

## Monitoring

Once the load is running, check the dashboards:
```plaintext
Service	         URL	                    Description
Grafana	         http://localhost:3000	    Login/Pass: admin / admin. Main RPS & Error dashboards.
Prometheus	     http://localhost:9090	    Raw metrics (rate(soundchain_processed_messages_total[1m])).
ClickHouse     	 http://localhost:8123	    HTTP DB interface (for curl checks).
```
## Current Status & TODO

 * [x] Core: gRPC Server + Kafka Producer
 * [x] ETL: Async Worker
 * [x] Infrastructure: Docker Compose
 * [x] Monitoring: Grafana Dashboard
 * [ ] Tests: Unit & Integration tests
 * [ ] CI/CD: GitHub Actions pipeline