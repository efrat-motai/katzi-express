import functools
import json
import logging
from src.infrastructure.messaging.rabbitmq_base import RabbitmqBase

LOGGER = logging.getLogger(__name__)

class PurchaseConsumer(RabbitmqBase):

    def __init__(self, amqp_url, queue_name, routing_key, exchange, exchange_type, data_service):
        super().__init__(amqp_url, queue_name, routing_key, exchange, exchange_type)
        self.should_reconnect = False
        self.was_consuming = False
        self._consumer_tag = None
        self._consuming = False
        self._prefetch_count = 1
        self.service = data_service

    def on_connection_open_error(self, _unused_connection, err):
        LOGGER.error('Connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        self._consuming = False
        self.service.close_connections()
        if self._stopping:
            self._connection.ioloop.stop()
            LOGGER.warning("Connection closed")
        else:
            LOGGER.warning('Connection closed, reconnect necessary: %s', reason)
            self.reconnect()

    def reconnect(self):
        self.should_reconnect = True
        self.stop()

    def on_setup_ready(self):
        self.set_qos()

    def set_qos(self):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        LOGGER.info('QOS set to: %d', self._prefetch_count)
        self.start_consuming()

    def start_consuming(self):
        LOGGER.info('Issuing consumer related RPC commands')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._consumer_tag = self._channel.basic_consume(
            self.queue_name, self.on_message)
        self.was_consuming = True
        self._consuming = True

    def on_consumer_cancelled(self, method_frame):
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        message:dict = json.loads(body)
        success = self.service.process(message,body)
        if success:
            self.acknowledge_message(basic_deliver.delivery_tag)
        else:
            self.reject_message(basic_deliver.delivery_tag, )

    def acknowledge_message(self, delivery_tag):
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def reject_message(self, delivery_tag, requeue=True):
        LOGGER.warning('Rejecting message %s (requeue=%s)', delivery_tag, requeue)
        self._channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)

    def stop_consuming(self):
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            cb = functools.partial(
                self.on_cancel_ok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancel_ok(self, _unused_frame, userdata):
        self._consuming = False
        LOGGER.info(
            'RabbitMQ acknowledged the cancellation of the consumer: %s',
            userdata)
        self.close_channel()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        if not self._stopping:
            self._stopping = True
            LOGGER.info('Stopping')
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            LOGGER.info('Stopped')
