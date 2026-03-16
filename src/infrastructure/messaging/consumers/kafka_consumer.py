import logging
from retry import retry
from confluent_kafka import KafkaException, Consumer

LOGGER = logging.getLogger(__name__)


class KafkaConsumer:

    def __init__(self, data_service):
        self.consumer = None
        self.service = data_service

    @retry(tries=5, delay=2, backoff=2, exceptions=KafkaException)
    def connect(self, consumer_config, topics):
        self.consumer = Consumer(**consumer_config)
        self.consumer.subscribe(topics)
        LOGGER.info('Consumer connected successfully')

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
                    self.service.process(data)

        except KeyboardInterrupt:
            LOGGER.warning('Aborted by user')
        except Exception as e:
            LOGGER.error(f"Unexpected error in consumer loop: {e}")

        finally:
            self.consumer.close()
            LOGGER.info('Consumer closed\n')
