import logging
from confluent_kafka import Consumer, KafkaException

LOGGER = logging.getLogger(__name__)


class KafkaConsumer:

    def __init__(self, kafka_config, topics):
        self.consumer = None
        self.connect(kafka_config,topics)

    def connect(self, kafka_config, topics):
        self.consumer = Consumer(**kafka_config)
        self.consumer.subscribe(topics)


    def consume_notification(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    LOGGER.error(f"Kafka error: {msg.error()}")
                    continue
                else:
                    data = msg.value().decode('utf-8')
                    LOGGER.info(f"Received message: {data}")

        except KeyboardInterrupt:
            LOGGER.warning('%% Aborted by user\n')
        except Exception as e:
            LOGGER.error(f"Unexpected error in consumer loop: {e}")

        finally:
            self.consumer.close()
            LOGGER.info('Consumer closed\n')
