import functools
import json
import logging
import pika

LOGGER = logging.getLogger(__name__)


class PurchaseConsumer:

    def __init__(self, amqp_url, queue_name, routing_key=None, exchange='', exchange_type='direct', redis_factory =None):
        self.should_reconnect = False
        self.was_consuming = False
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self._queue_name = queue_name
        self._routing_key = routing_key or queue_name
        self._exchange = exchange
        self._exchange_type = exchange_type
        self._consuming = False
        self._prefetch_count = 1
        self._redis_repo = redis_factory()

    def connect(self):
        LOGGER.info('Connecting to %s', self._url)
        return pika.SelectConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            LOGGER.info('Connection is closing or already closed')
        else:
            LOGGER.info('Closing connection')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        LOGGER.info('Connection opened')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_connection_open_error(self, _unused_connection, err):
        LOGGER.error('Connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._redis_repo:
            self._redis_repo.close_connection()
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reconnect necessary: %s', reason)
            self.reconnect()

    def reconnect(self):
        self.should_reconnect = True
        self.stop()

    def on_channel_open(self, channel):
        LOGGER.info(f"Channel opened")
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self.setup_exchange(self._exchange)

    def setup_exchange(self, exchange_name):
        LOGGER.info('Declaring exchange: %s', exchange_name)
        cb = functools.partial(self.on_exchange_declare_ok,
                               userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self._exchange_type,
            callback=cb)

    def on_exchange_declare_ok(self, _unused_frame, userdata):
        LOGGER.info('Exchange declared: %s', userdata)
        self.setup_queue(self._queue_name)

    def setup_queue(self, queue_name):
        LOGGER.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(queue=queue_name,
                                    durable=True,
                                    callback=self.on_queue_declare_ok)

    def on_queue_declare_ok(self, _unused_frame):
        LOGGER.info('Binding %s to %s with %s', self._exchange, self._queue_name,
                    self._routing_key)
        self._channel.queue_bind(self._queue_name,
                                 self._exchange,
                                 routing_key=self._routing_key,
                                 callback=self.on_bind_ok)

    def on_channel_closed(self, channel, reason):
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self.close_connection()

    def on_bind_ok(self, _unused_frame):
        LOGGER.info("Queue bound.")
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
            self._queue_name, self.on_message)
        self.was_consuming = True
        self._consuming = True

    def on_consumer_cancelled(self, method_frame):
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        purchase_notification:dict = json.loads(body)
        success = self._redis_repo.add_notification(purchase_notification)
        if success:
            self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

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

    def close_channel(self):
        LOGGER.info('Closing the channel')
        self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        if not self._closing:
            self._closing = True
            LOGGER.info('Stopping')
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            LOGGER.info('Stopped')
