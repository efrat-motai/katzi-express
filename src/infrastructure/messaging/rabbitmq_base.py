import pika
import logging

from pika.exceptions import AMQPConnectionError
from pika.exchange_type import ExchangeType
from abc import ABC, abstractmethod
from retry import retry

LOGGER = logging.getLogger(__name__)


class RabbitmqBase(ABC):
    def __init__(self, amqp_url, queue_name, routing_key=None, exchange='', exchange_type=ExchangeType.direct):
        self._connection = None
        self._channel = None
        self._url = amqp_url
        self.queue_name = queue_name
        self.routing_key = routing_key or queue_name
        self.exchange = exchange
        self.exchange_type = exchange_type
        self._stopping = False

    @retry(tries=-1, delay=2, backoff=2, exceptions=AMQPConnectionError)
    def connect(self):
        LOGGER.info('Connecting to %s', self._url)
        self._connection = None
        self._channel = None
        self._connection = pika.BlockingConnection(pika.URLParameters(self._url))
        self._channel = self._connection.channel()
        self.setup_exchange()
        self.on_setup_ready()

    def setup_exchange(self):
        LOGGER.info('Declaring exchange: %s', self.exchange)
        self._channel.exchange_declare(
            exchange=self.exchange,
            durable=True,
            exchange_type=self.exchange_type)
        self.setup_queue()

    def setup_queue(self):
        LOGGER.info('Declaring queue %s', self.queue_name)
        self._channel.queue_declare(queue=self.queue_name, durable=True)
        self.setup_bind()

    def setup_bind(self):
        LOGGER.info('Binding %s to %s with %s', self.exchange, self.queue_name, self.routing_key)
        self._channel.queue_bind(self.queue_name,
                                 self.exchange,
                                 routing_key=self.routing_key)

    @abstractmethod
    def run(self):
        pass

    def stop(self):
        LOGGER.info('Stopping...')
        self._stopping = True
        if self._channel and self._channel.is_open:
            self._channel.close()
            LOGGER.info('Channel closed')
        if self._connection and self._connection.is_open:
            self._connection.close()
            LOGGER.info('Connection closed')

    @abstractmethod
    def on_setup_ready(self):
        pass
