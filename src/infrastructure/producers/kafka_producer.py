import logging
from confluent_kafka import Producer

LOGGER = logging.getLogger(__name__)

class PurchaseKafkaProducer:

    def __init__(self, kafka_config):
        self.producer = Producer(**kafka_config)

    def publish_notification(self, topic, key, value):
        try:
            self.producer.produce(
                topic,
                key=str(key),
                value=value,
                on_delivery=self.delivery_callback
            )
            self.producer.poll(0)
            self.producer.flush(1)
            return True
        except Exception as e:
            LOGGER.error(f"Failed to produce to Kafka: {e}")
            return False

    def delivery_callback(self, err, msg):
        if err is not None:
            LOGGER.error(f'Message delivery failed: {err}')
        else:
            LOGGER.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')


