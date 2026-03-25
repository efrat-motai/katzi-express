import json
import logging
import signal
from json import JSONDecodeError
from src.infrastructure.messaging.rabbitmq_base import RabbitmqBase

LOGGER = logging.getLogger(__name__)


class PurchaseConsumer(RabbitmqBase):

    def __init__(self, amqp_url, queue_name, routing_key, exchange, exchange_type, data_service, prefetch_count=1):
        super().__init__(amqp_url, queue_name, routing_key, exchange, exchange_type)
        self.should_reconnect = False
        self._consumer_tag = None
        self._consuming = False
        self._prefetch_count = prefetch_count
        self.service = data_service
        signal.signal(signal.SIGINT, self._handle_exit)

    def on_setup_ready(self):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count)

    def on_message(self, channel, method_frame, properties, body):
        LOGGER.info('Received message # %s from %s: %s',
                    method_frame.delivery_tag, properties.app_id, body)
        try:
            message: dict = json.loads(body)
            success = self.service.dispatch_to_kafka(message, body)
            if success:
                self.acknowledge_message(method_frame.delivery_tag)
            else:
                self.reject_message(method_frame.delivery_tag)
        except JSONDecodeError as e:
            LOGGER.error("Invalid JSON format for message %s: %s", method_frame.delivery_tag, e)
            self._channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def reject_message(self, delivery_tag, requeue=True):
        LOGGER.warning('Rejecting message %s (requeue=%s)', delivery_tag, requeue)
        self._channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)

    def _handle_exit(self, signum, frame):
        LOGGER.info("Exit signal received (%s). Graceful shutdown...", signum)
        if self._connection and self._connection.is_open:
            self._connection.add_callback_threadsafe(self.stop)

    def run(self):
        while not self._stopping:
            try:
                self.connect()
                self._channel.basic_consume(self.queue_name, self.on_message)
                self._channel.start_consuming()
            except KeyboardInterrupt:
                self.stop()
                break
            except Exception as e:
                LOGGER.error("Main loop crashed: %s", e)

    def stop(self):
        LOGGER.info('Stopping consumer...')
        self._stopping = True
        try:
            if self._channel and self._channel.is_open:
                self._channel.stop_consuming()
                self._channel.close()
            if self._connection and self._connection.is_open:
                self._connection.close()
            self.service.close_connections()
        except Exception as e:
            LOGGER.debug("Error during close: %s", e)

        LOGGER.info('Stopped.')
