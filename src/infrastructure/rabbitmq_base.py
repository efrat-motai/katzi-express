import abc
import functools
from abc import ABC
import pika
import logging

from pika.exchange_type import ExchangeType

LOGGER = logging.getLogger(__name__)


class RabbitmqBase(ABC):
    RECONNECT_DELAY = 5

    def __init__(self, amqp_url, queue_name, routing_key=None, exchange='', exchange_type=ExchangeType.direct):
        self._connection = None
        self._channel = None
        self._url = amqp_url
        self.queue_name = queue_name
        self.routing_key = routing_key or queue_name
        self.exchange = exchange
        self.exchange_type = exchange_type
        self._stopping = False

    @abc.abstractmethod
    def run(self):
        pass

    @abc.abstractmethod
    def stop(self):
        pass

    def connect(self):
        LOGGER.info('Connecting to %s', self._url)
        self._connection = None
        self._channel = None
        return pika.SelectConnection(
            pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def on_connection_open(self, _unused_connection):
        LOGGER.info("Connection opened")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_connection_open_error(self, conn, error):
        LOGGER.error('Connection open failed, reopening in %i seconds: %s', self.RECONNECT_DELAY, error)
        self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_connection_closed(self, conn, reason):
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in %i seconds: %s', self.RECONNECT_DELAY,
                           reason)
            self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_channel_open(self, channel):
        LOGGER.info(f"Channel opened")
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self.setup_exchange(self.exchange)

    def setup_exchange(self, exchange_name):
        LOGGER.info('Declaring exchange: %s', exchange_name)
        cb = functools.partial(self.on_exchange_declare_ok,
                               userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.exchange_type,
            callback=cb)

    def on_exchange_declare_ok(self, _unused_frame, userdata):
        LOGGER.info('Exchange declared: %s', userdata)
        self.setup_queue(self.queue_name)

    def setup_queue(self, queue_name):
        LOGGER.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(queue=queue_name,
                                    durable=True,
                                    callback=self.on_queue_declare_ok)

    def on_queue_declare_ok(self, _unused_frame):
        LOGGER.info('Binding %s to %s with %s', self.exchange, self.queue_name,
                    self.routing_key)
        self._channel.queue_bind(self.queue_name,
                                 self.exchange,
                                 routing_key=self.routing_key,
                                 callback=self.on_bind_ok)

    def on_channel_closed(self, channel, reason):
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self._channel = None
        if not self._stopping:
            self.close_connection()

    def on_bind_ok(self, _unused_frame):
        LOGGER.info("Queue bound.")
        self.on_setup_ready()

    @abc.abstractmethod
    def on_setup_ready(self):
        pass

    def close_channel(self):
        if self._channel is not None:
            LOGGER.info('Closing the channel')
            self._channel.close()

    def close_connection(self):
        if self._connection.is_closing or self._connection.is_closed:
            LOGGER.info('Connection is closing or already closed')
        else:
            LOGGER.info('Closing connection')
            self._connection.close()
