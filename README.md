# 🚀katzi-express 

## 📦 Delivery Guarantee
The system implements the **At-Least-Once** delivery model. This ensures that every purchase message is successfully processed and stored before it is removed from the queue.

---

## 🛡️ Failure Tolerance & Data Safety
To prevent data loss during processing, the following measures were taken:

* **RabbitMQ Durability:** All exchanges and queues are configured as `durable`.
* **Message Persistence:** Messages are sent with `delivery_mode=2` (persistent), ensuring they are saved to the disk and survive a broker restart.
* **Publisher Confirms (Producer Side):** The Producer tracks outgoing messages and waits for a confirmation from RabbitMQ. If the connection drops before an acknowledgment is received, the messages are held in memory and resent upon reconnection.
* **Manual Acknowledgments (Consumer Side):** The Consumer sends a `basic_ack` **only** after both Redis and Kafka operations are successfully completed. If any step fails, the message remains in the queue.
---

## 🔄 Disaster Recovery & Retries
The system is designed to recover from infrastructure failures automatically:

* **Consumer Reconnection Logic:** The consumer implements a specialized `ConsumerReconnector` with an **Exponential Backoff** strategy. 
  - If a connection fails, it waits progressively longer (up to 30 seconds) before retrying to avoid overloading the broker.
  - If the consumer was actively processing messages before the failure, it resets the delay to 0 for immediate recovery.
* **Producer Retries:** The producer uses a simpler retry mechanism, attempting to re-establish the connection every 5 seconds upon failure.
* **Service Resilience (Redis/Kafka):** Since these clients have internal retry mechanisms, the application focuses on error handling. If a write fails, the message is NOT acknowledged in RabbitMQ, ensuring it stays safe until the dependency is back online.
---

## 🔌 Graceful Shutdown
The application handles manual termination using a `KeyboardInterrupt` (Ctrl+C) block to ensure a clean exit:

1.  **Stop Consuming:** The consumer first triggers `basic_cancel` to stop receiving new messages from the broker.
2.  **Finish Task:** It completes the processing of the current message being handled.
3.  **Cleanup:** It flushes Kafka buffers and closes all active connections (Redis, Kafka, RabbitMQ) before the process finally exits.

---

## 🧪 Resiliency Test Results (Docker "Kill" Tests)

I performed manual "Kill Tests" by stopping Docker containers during runtime to verify the system's stability:

### 1. 🐰 RabbitMQ is Down
* **Scenario:** Stopping the RabbitMQ container while the system is running.
* **Result:** Producer and Consumer logs show "Connection closed, reconnecting...". 
* **Recovery:** Once the container was restarted, both components reconnected automatically. **Zero messages were lost.**

### 2. 🔴 Redis is Down
* **Scenario:** Stopping the Redis container during message processing.
* **Result:** The Consumer fails to persist the purchase data to Redis and catches the exception.
* **Recovery**: The Consumer catches the exception and explicitly rejects the message using basic_nack with requeue=True. This ensures the message is returned to the queue and processed again once the service is restored.

### 3. 🏁 Kafka is Down
* **Scenario:** Stopping the Kafka broker while the consumer is trying to produce a notification.
* **Result:** The Kafka `flush()` or `delivery_callback` returns an error/timeout.
* **Recovery**: The Consumer catches the exception and explicitly rejects the message using basic_nack with requeue=True. This ensures the message is returned to the queue and processed again once the service is restored.
