import logging
from confluent_kafka import Consumer, KafkaException

LOGGER = logging.getLogger(__name__)


class KafkaConsumer:

    def __init__(self, kafka_config, topics):
        self.consumer = Consumer(**kafka_config)
        self.consumer.subscribe(topics, on_assign=self.print_assignment)



    def consume_notification(self, topic, key, value):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    LOGGER.info(
                        '%% %s [%d] at offset %d with key %s:\n'
                        % (msg.topic(), msg.partition(), msg.offset(), str(msg.key()))
                    )
                    print(msg.value())
                    # Store the offset associated with msg to a local cache.
                    # Stored offsets are committed to Kafka by a background thread every 'auto.commit.interval.ms'.
                    # Explicitly storing offsets after processing gives at-least once semantics.
                    self.consumer.store_offsets(msg)

        except KeyboardInterrupt:
           LOGGER.error('%% Aborted by user\n')

        finally:
            self.consumer.close()

