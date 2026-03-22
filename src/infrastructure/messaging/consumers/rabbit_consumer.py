import json
import logging

from pika.exceptions import IncompatibleProtocolError, AMQPConnectionError

from src.infrastructure.messaging.rabbitmq_base import RabbitmqBase

LOGGER = logging.getLogger(__name__)


class PurchaseConsumer(RabbitmqBase):

    def __init__(self, amqp_url, queue_name, routing_key, exchange, exchange_type, data_service, prefetch_count = 1):
        super().__init__(amqp_url, queue_name, routing_key, exchange, exchange_type)
        self.should_reconnect = False
        self.was_consuming = False
        self._consumer_tag = None
        self._consuming = False
        self._prefetch_count = prefetch_count
        self.service = data_service

    def on_setup_ready(self):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count)

    def start_consuming(self):
        LOGGER.info('Starting to consume messages from %s', self.queue_name)
        for method_frame, properties, body in self._channel.consume(self.queue_name, inactivity_timeout=1):
            if self._stopping:
                break

            if method_frame:
                self.on_message(method_frame=method_frame, properties=properties, body=body)

    def on_message(self, method_frame, properties, body):
        LOGGER.info('Received message # %s from %s: %s',
                    method_frame.delivery_tag, properties.app_id, body)
        message: dict = json.loads(body)
        success = self.service.process(message, body)
        if success:
            self.acknowledge_message(method_frame.delivery_tag)
        else:
            self.reject_message(method_frame.delivery_tag, )

    def acknowledge_message(self, delivery_tag):
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def reject_message(self, delivery_tag, requeue=True):
        LOGGER.warning('Rejecting message %s (requeue=%s)', delivery_tag, requeue)
        self._channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)

    def run(self):
        while not self._stopping:
            try:
                self.connect()
                self.start_consuming()
            except KeyboardInterrupt:
                self.stop()
                break
            except (AMQPConnectionError,IncompatibleProtocolError) as e:
                LOGGER.error("Connection error: %s", e)

            except Exception as e:
                LOGGER.error("Main loop crashed: %s", e)

    def stop(self):
        LOGGER.info('Stopping consumer...')
        self._stopping = True
        try:
            if self._channel and self._channel.is_open:
                self._channel.cancel()
                self._channel.close()
            if self._connection and self._connection.is_open:
                self._connection.close()

        except Exception as e:
            LOGGER.debug("Error during close: %s", e)

        self.service.close_connections()
        LOGGER.info('Stopped.')
