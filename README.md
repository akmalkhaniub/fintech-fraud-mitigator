# App 9: Fintech "Instant Fraud" Mitigator (Voice)

## Concept
A high-priority outbound security agent that calls users within seconds of a suspicious transaction.

## Workflow
1.  **Trigger:** Monitors a Kafka/RabbitMQ stream for "Suspicious Transaction" flags.
2.  **Outbound Call:** Calls the user immediately.
3.  **Verification:** Verifies the user's identity using MFA or voice biometrics.
4.  **Mitigation:** 
    - If user confirms fraud: Agent freezes the card via API and flags the transaction for chargeback.
    - If user confirms legitimate: Agent whitelists the transaction.
5.  **Human Escalate:** If the user is confused or the situation is complex, the agent transfers the call to a senior fraud investigator.

## Tech Stack
- **Language:** Go
- **Messaging:** Apache Kafka (for low-latency events)
- **Telephony:** Twilio Voice (Webhooks)
- **TTS:** Play.ht (Ultra-realistic voices)
- **Identity:** Auth0 + Voice Biometrics (Pindrop)
- **Caching:** Redis (for session state)
