# Fintech Fraud Mitigator

A real-time fraud detection pipeline in Go that scores transactions against configurable risk rules, generates alerts, and triggers outbound verification calls for high-risk events.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        TRANSACTION SOURCES                               │
│          (Payment Gateway, POS Terminal, Mobile App, Wire Transfer)       │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │  POST /transactions
                                  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                           HTTP API (net/http)                            │
│                                                                          │
│  POST /transactions         — ingest a transaction event                 │
│  GET  /alerts               — list all fraud alerts                      │
│  GET  /alerts/{id}          — get alert details                          │
│  POST /alerts/{id}/resolve  — resolve alert (confirm fraud / legit)      │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │  publish to "transactions" topic
                                  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        EVENT BUS (Kafka Interface)                       │
│                                                                          │
│  "transactions" ──────────────▶ FraudPipeline.processTransaction()       │
│                                        │                                 │
│                                        ▼                                 │
│                              ┌───────────────────┐                       │
│                              │   RISK ENGINE      │                      │
│                              │                    │                      │
│                              │  Rule: high_amount │                      │
│                              │  Rule: location    │                      │
│                              │  Rule: velocity    │                      │
│                              │  Rule: odd_hours   │                      │
│                              │                    │                      │
│                              │  Output:           │                      │
│                              │   score (0.0-1.0)  │                      │
│                              │   risk level       │                      │
│                              │   signal list      │                      │
│                              └────────┬──────────┘                       │
│                                       │                                  │
│                    ┌──────────────────┼──────────────────┐               │
│                    │                  │                  │               │
│                    ▼                  ▼                  ▼               │
│              score < 0.3       0.3 ≤ score < 0.7   score ≥ 0.7         │
│              ┌─────────┐      ┌─────────────┐     ┌──────────────┐     │
│              │ APPROVE  │      │ CREATE ALERT │     │ CREATE ALERT │     │
│              │ (no-op)  │      │ status:      │     │ status:      │     │
│              └─────────┘      │ pending      │     │ calling      │     │
│                               └──────┬──────┘     └──────┬───────┘     │
│                                      │                    │             │
│  "fraud_alerts" ◀────────────────────┴────────────────────┘             │
└──────────────────────────────────────────────────────────────────────────┘
                                       │
                          ┌────────────┼────────────────┐
                          ▼            ▼                ▼
                   ┌───────────┐ ┌──────────┐  ┌──────────────┐
                   │  Twilio   │ │  Redis   │  │  Dashboard   │
                   │  Outbound │ │  Session │  │  / Webhook   │
                   │  Call     │ │  Cache   │  │  Consumer    │
                   └───────────┘ └──────────┘  └──────────────┘
```

## Alert Resolution Flow

```
          Suspicious Transaction Detected
                     │
                     ▼
              ┌──────────────┐
              │  Risk Score  │
              │  Assessment  │
              └──────┬───────┘
                     │
           ┌─────────┼──────────┐
           ▼         ▼          ▼
        < 0.3     0.3-0.7    ≥ 0.7
       APPROVE    PENDING   AUTO-CALL
                     │          │
                     ▼          ▼
              ┌──────────────────────┐
              │   Outbound Call to   │
              │   Account Holder     │
              └──────────┬───────────┘
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
         confirmed   confirmed   no answer
          fraud      legitimate     │
              │          │          ▼
              ▼          ▼      escalate to
         FREEZE      WHITELIST  human fraud
         CARD        TRANSACTION investigator
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Language | Go 1.22 | High-throughput, low-latency processing |
| Event Bus | In-memory (Kafka interface) | Pub/sub for transaction events |
| Risk Engine | Rule-based scoring | Configurable fraud detection rules |
| State | In-memory (Redis interface) | Alert storage and session cache |
| API | net/http | REST endpoints for ingest and resolution |
| Telephony | Twilio (integration point) | Outbound verification calls |

## Quick Start

```bash
go run main.go
```

## API Examples

### Ingest a transaction
```bash
curl -X POST http://localhost:8080/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "usr_123",
    "amount": 6500.00,
    "currency": "USD",
    "merchant": "Unknown Electronics Store",
    "location": "NG",
    "card_last4": "4242"
  }'
```

### List alerts
```bash
curl http://localhost:8080/alerts
```

### Resolve an alert
```bash
curl -X POST http://localhost:8080/alerts/ALT-TX-123/resolve \
  -H "Content-Type: application/json" \
  -d '{"outcome": "confirmed_fraud", "verified_by": "mfa"}'
```

## Risk Rules

| Rule | Score Weight | Trigger |
|------|-------------|---------|
| `high_amount` | +0.2 / +0.4 | >$1000 / >$5000 |
| `unusual_location` | +0.3 | Transaction from high-risk region |
| `velocity` | configurable | Too many transactions in time window |
| `odd_hours` | +0.15 | Transaction between 1am-5am |

Rules are additive — a $6000 transaction from Nigeria at 3am scores 0.85 (critical).

## Design Decisions

- **Event-driven**: Transactions are published to a topic and consumed asynchronously, decoupling ingestion from scoring.
- **Pluggable rules**: Each risk rule is a function — add new signals (IP reputation, device fingerprint) without changing the pipeline.
- **In-memory with interfaces**: The event bus and store use the same patterns as Kafka/Redis but run in-process for zero-dependency development.
- **Graceful shutdown**: SIGINT/SIGTERM triggers orderly pipeline drain and HTTP server shutdown.
