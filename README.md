# kafka-exactly-once-cashback

Exactly-once cashback processing pipeline — Apache Kafka 4.x + Spring Boot 3.x + PostgreSQL 17.

## The Problem

Partner sends a conversion postback. Service crashes after writing to Kafka but before writing to PostgreSQL. On restart — does the user get credited twice?

## Three-Layer Solution

```
Layer 1  enable.idempotence=true + transactional.id
         Kafka deduplicates retried sends at the broker level

Layer 2  isolation.level=read_committed + AckMode.MANUAL_IMMEDIATE
         Offset committed only after DB write succeeds
         Crash before commit = safe redelivery, not data loss

Layer 3  UNIQUE constraint on transactionId in PostgreSQL
         existsByTransactionId() before every INSERT
         Race between two consumer threads = constraint violation, one wins
```

## Flow

```
POST /postback
      |
      v
TransactionalCashbackProducer  (executeInTransaction)
      |
      v
Kafka: conversions
      |
      +---> ExactlyOnceCashbackConsumer  (read_committed, manual ACK)
      |              |
      |              +--> existsByTransactionId()
      |              +--> INSERT cashback_transactions
      |              +--> ack.acknowledge()
      |              +--> emit to cashback-confirmed
      |
      +---> (3 retries: 1s, 2s, 4s) --> conversions-dlt
```

## Run

```bash
kafka-storage format -t $(kafka-storage random-uuid) -c /opt/homebrew/etc/kafka/server.properties
brew services start kafka
brew services start postgresql@17
psql postgres -c "CREATE DATABASE cashbackdb;"
psql postgres -c "CREATE USER cashbackuser WITH PASSWORD 'cashback123';"
psql postgres -c "GRANT ALL PRIVILEGES ON DATABASE cashbackdb TO cashbackuser;"
./mvnw spring-boot:run
```

## Test

```bash
curl -X POST "http://localhost:8080/postback?transactionId=tx-001&userEmail=mo@shoop.de&partnerId=eBay&amount=100&rate=0.03"

curl "http://localhost:8080/balance/mo@shoop.de"

curl -X POST "http://localhost:8080/postback?transactionId=tx-001&userEmail=mo@shoop.de&partnerId=eBay&amount=100&rate=0.03"

curl "http://localhost:8080/balance/mo@shoop.de"
```

Second postback with same `transactionId` — balance stays €3.00, not €6.00.

```bash
curl -X POST "http://localhost:8080/simulate?partnerId=Douglas&amount=50&rate=0.10"
```

## Stack

- Java 21
- Spring Boot 3.4 / Spring Kafka 3.x
- Apache Kafka 4.x (KRaft)
- PostgreSQL 17
