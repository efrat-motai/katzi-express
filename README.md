# 🚀 Katzi-Express

## 🏗️ Architecture Overview
The system manages real-time purchase event streams using **RabbitMQ** as a primary buffer, **Kafka** for event distribution, and **Redis** for fast state management (Hot Products). 
The architecture is designed for **Maximum Resilience (Zero Data Loss)** and high performance.

---

## 📦 Data Consistency & Race Conditions
To ensure data accuracy under high concurrency, I implemented infrastructure-level solutions:

* **Atomic Operations (Redis):** Using the `ZINCRBY` command ensures that the "Hot Product" counter updates are **atomic**. This prevents Race Conditions where multiple consumers update the same product simultaneously, avoiding lost counts.
* **Deterministic Partitioning (Kafka):** Every message is sent to Kafka using the `Product_ID` as the **Key**. This guarantees that all purchases for a specific product always land in the same **Partition**.
    * **Result:** Maintains strict **Message Ordering** and allows for seamless scalability within Consumer Groups.

---

## 🛡️ Infrastructure Resilience & Profiles
The system uses **Polymorphic Bootstrapping** to adapt infrastructure behavior based on the runtime context (via YAML configuration):

### 1. API Profile (Low Latency)
* **Fail-Fast:** Configured with a short `socket_timeout` (0.5s). If Redis is slow or unavailable, the API "gives up" quickly and returns an empty state to keep the user experience smooth.
* **Startup Validation:** The API performs an active `ping()` during the **Lifespan** stage. If the infrastructure is down, the application will refuse to start (**Fail-Fast**).

### 2. Consumer Profile (High Durability)
* **Exponential Backoff:** The consumer is equipped with an aggressive retry mechanism (up to 15 attempts). If Redis or Kafka are unavailable, the consumer "fights" for the message, ensuring the business logic is completed without crashing.
* **Hierarchical Acknowledge Chain:** * **RabbitMQ:** Sends `basic_ack` only after a successful Kafka dispatch.
    * **Kafka:** Commits the **Manual Offset** only after a successful Redis update (`ZINCRBY`).
    * This chain ensures that responsibility for the data is only transferred once the next hop is secured.

---

## 🔄 Delivery Guarantee & Safety
* **At-Least-Once Delivery:** I guarantee that no message is lost. A message is only removed from the source (RabbitMQ/Kafka) after it is safely persisted in the destination (Kafka/Redis).
* **Manual Control:** I disabled all "Auto-Ack" and "Auto-Commit" features. Every step of the message lifecycle is manually acknowledged only after verification.
* **Poison Message Protection:** Malformed JSON messages are identified and immediately acknowledged to prevent infinite retry loops, while logging the error for manual inspection.

---

## 🛑 Graceful Shutdown
The system implements a structured shutdown protocol (`SIGINT` / `Ctrl+C`) to ensure data integrity:

1.  **Stop Ingestion:** Triggers `channel.cancel()` to stop receiving new messages from RabbitMQ.
2.  **Task Completion:** The consumer finishes processing the current "in-flight" message.
3.  **Kafka Buffer Flush:** Executes `producer.flush()` to ensure all buffered messages are physically sent to the Kafka brokers.
4.  **Cleanup:** Safely closes all active connections (Redis, Kafka, RabbitMQ) to prevent "zombie" connections or resource leaks.

---

## 🧪 Resiliency Test Results ("The Kill Tests")
* **Kill Redis:** The consumer enters its retry loop and waits for the service to return. The message remains "Unacked" in RabbitMQ until the operation completes.
* **Kill Kafka:** The dispatch fails, the consumer rejects the message, and it is automatically requeued in RabbitMQ for a later retry.
* **Kill Consumer:** Thanks to manual acks, any message being processed during a crash is automatically returned to RabbitMQ by the broker (**Automatic Requeue**).