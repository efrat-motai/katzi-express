import logging
from confluent_kafka import Consumer

LOGGER = logging.getLogger(__name__)


class KafkaConsumer:

    def __init__(self, data_service):
        self.consumer = None
        self.service = data_service

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
                    processed = False
                    while not processed:
                        if self.service.handle_purchase_event(data):
                            self.consumer.commit(asynchronous=False)
                            LOGGER.info(f"Message processed and committed: {data}")
                            processed = True
                        else:
                            LOGGER.error(f"Failed to process message. Skipping commit for: {data}")

        except KeyboardInterrupt:
            LOGGER.warning('Aborted by user')
        except Exception as e:
            LOGGER.error(f"Unexpected error in consumer loop: {e}")

        finally:
            self.consumer.close()
            LOGGER.info('Consumer closed\n')
