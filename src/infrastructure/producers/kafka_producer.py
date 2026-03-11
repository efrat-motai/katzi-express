import logging
from confluent_kafka import Producer

LOGGER = logging.getLogger(__name__)


class KafkaProducer:

    def __init__(self, kafka_config):
        self.producer = Producer(**kafka_config)
        self.delivery_status = False

    def publish_notification(self, topic, key, value) -> bool:
        self.delivery_status = True
        try:
            self.producer.produce(
                topic,
                key=str(key),
                value=value,
                on_delivery=self.delivery_callback
            )
            self.producer.flush()
            return self.delivery_status
        except Exception as e:
            LOGGER.error(f"Failed to produce to Kafka: {e}")
            return False

    def delivery_callback(self, err, msg):
        if err is not None:
            LOGGER.error(f'Message delivery failed: {err}')
            self.delivery_status = False
        else:
            LOGGER.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')
            self.delivery_status = True

    def close_connection(self):
        self.producer.flush()
        LOGGER.info(f"Successfully closed Kafka connection")
