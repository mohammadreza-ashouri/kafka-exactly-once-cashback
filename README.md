# kafka-exactly-once-cashback

Demonstrates exactly-once semantics in a cashback event processing pipeline using **Apache Kafka 4.x** and **Spring Boot 3.x**.

## The Problem

A partner (eBay, H&M, Douglas) sends a conversion postback: *"user bought €100 of goods."*

The cashback service crashes **after writing to Kafka but before writing to PostgreSQL**.

On restart — does the user get credited **twice**? Or does the credit get **lost**?

Neither. This project solves it.

## The Solution — Three-Layer Exactly-Once Guarantee

```
Layer 1 — Kafka idempotent producer
          enable.idempotence=true + transactional.id
          → Kafka deduplicates retried sends using producer sequence numbers
          → A crashed producer cannot produce duplicate messages

Layer 2 — read_committed consumer + manual offset commit
          isolation.level=read_committed
          → Consumer only reads messages from fully committed transactions
          → Offset committed ONLY after successful PostgreSQL write
          → Crash after DB write but before offset commit → safe redelivery

Layer 3 — PostgreSQL idempotency key
          UNIQUE constraint on transactionId column
          → existsByTransactionId() check before every INSERT
          → Race condition between two consumer threads → constraint violation → one wins
          → User is credited exactly once under all failure scenarios
```

## Architecture

```
eBay / H&M / Douglas
      │
      ▼
POST /postback ──────────────────────────────────────────────────────────────────────────────────────────────────────────
      │                                                                                                    ▲
      ▼                                                                                                    │
TransactionalCashbackProducer                                                                    202 Accepted (async)
      │
      ▼
Kafka topic: "conversions"  (idempotent, transactional)
      │
      ├──► ExactlyOnceCashbackConsumer (read_committed, manual ACK)
      │           │
      │           ├── [Layer 3] existsByTransactionId() check
      │           ├── INSERT cashback_transactions (UNIQUE constraint)
      │           ├── Commit Kafka offset
      │           └── Emit to "cashback-confirmed" topic
      │
      └──► (on failure after 3 retries) → "conversions-dlt" (Dead Letter)
                                                │
                                                └── DLQ consumer → alert + manual replay
```

## Key Design Decisions

| Decision | Reason |
|---|---|
| `enable.idempotence=true` | Deduplicates producer retries at the Kafka level |
| `transactional.id` set | Enables full Kafka transaction support |
| `isolation.level=read_committed` | Consumer never sees uncommitted (crashed) messages |
| `AckMode.MANUAL_IMMEDIATE` | Offset committed only after DB write succeeds |
| `SERIALIZABLE` isolation on DB transaction | Prevents phantom reads during concurrent credits |
| `@Version` on entity | Optimistic locking — detects concurrent balance updates |
| Rate snapshotted at processing time | Partner rate change does not affect existing pending transactions |
| `ExponentialBackOff(1s, 2x, max 3)` | Transient failures retried; persistent failures routed to DLQ |

## Running Locally

**Prerequisites:** Java 21, Apache Kafka 4.x, PostgreSQL 17

```bash
# Start Kafka (KRaft mode — no Zookeeper)
kafka-storage format -t $(kafka-storage random-uuid) -c /opt/homebrew/etc/kafka/server.properties
brew services start kafka

# Start PostgreSQL and create database
brew services start postgresql@17
psql postgres -c "CREATE DATABASE cashbackdb;"
psql postgres -c "CREATE USER cashbackuser WITH PASSWORD 'cashback123';"
psql postgres -c "GRANT ALL PRIVILEGES ON DATABASE cashbackdb TO cashbackuser;"

# Run the application
./mvnw spring-boot:run
```

## Testing Exactly-Once Behavior

```bash
# Normal flow — credit €3 cashback
curl -X POST "http://localhost:8080/postback?transactionId=tx-001&userEmail=mo@shoop.de&partnerId=eBay&amount=100&rate=0.03"

# Check balance — should show €3.00 pending
curl "http://localhost:8080/balance/mo@shoop.de"

# Send SAME postback again — idempotency test
curl -X POST "http://localhost:8080/postback?transactionId=tx-001&userEmail=mo@shoop.de&partnerId=eBay&amount=100&rate=0.03"

# Balance STILL €3.00 — not €6.00. Exactly-once confirmed.
curl "http://localhost:8080/balance/mo@shoop.de"

# Quick simulation — random transactionId generated automatically
curl -X POST "http://localhost:8080/simulate?partnerId=Douglas&amount=50&rate=0.10"
```

## Topics

| Topic | Purpose | Retention |
|---|---|---|
| `conversions` | Incoming partner postbacks | 30 days |
| `cashback-confirmed` | Successfully credited events | 90 days |
| `conversions-dlt` | Dead letter — failed after max retries | 7 days |

## Tech Stack

- **Java 21** (virtual threads ready)
- **Spring Boot 3.4** (Spring Kafka 3.x)
- **Apache Kafka 4.x** (KRaft mode — no Zookeeper)
- **PostgreSQL 17** (ACID transactions, UNIQUE constraints)
- **Spring Data JPA** + Hibernate

## Author

Dr. Mohammadreza Ashouri — [bytescan.net](https://bytescan.net) | [GitHub](https://github.com/mohammadreza-ashouri)
