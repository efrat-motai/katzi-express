import time
import pika
import json
import logging
import random
from dataclasses import asdict
from src.infrastructure.messaging.rabbitmq_base import RabbitmqBase

LOGGER = logging.getLogger(__name__)


class RabbitProducer(RabbitmqBase):

    def __init__(self, amqp_url, queue_name, routing_key, exchange, exchange_type, generator_func):
        super().__init__(amqp_url, queue_name, routing_key, exchange, exchange_type)
        self._message_number = 0
        self._stopping = False
        self._generator_func = generator_func

    def run(self):
        while not self._stopping:
            self._message_number = 0
            try:
                self._connection = self.connect()
                self.schedule_next_message()
            except KeyboardInterrupt:
                self.stop()
                break
            except ConnectionAbortedError as e:
                LOGGER.error("Connection aborted: %s", e)
            except Exception as e:
                LOGGER.error("Main loop crashed: %s", e)

        LOGGER.info('Publisher loop finished.')

    def on_setup_ready(self):
        LOGGER.info("Enabling publisher confirms.")
        self._channel.confirm_delivery()

    def schedule_next_message(self):
        wait_time = random.randint(1, 6)
        LOGGER.info(f"Scheduling next publish in {wait_time}s...")
        time.sleep(wait_time)
        self.publish_message()

    def publish_message(self):
        if self._channel is None or not self._channel.is_open:
            return

        new_message = self._generator_func()
        payload = json.dumps(asdict(new_message), default=lambda o: o.isoformat())
        self._channel.basic_publish(self.exchange, self.routing_key,
                                    body=payload,
                                    properties=pika.BasicProperties(
                                        app_id="efrat-purchase-app",
                                        content_type="application/json",
                                        delivery_mode=2
                                    ))

        self._message_number += 1
        LOGGER.info('Published message # %i', self._message_number)
        self.schedule_next_message()
