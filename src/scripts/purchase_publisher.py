import pika
import json
import logging
import random
from dataclasses import asdict
from src.scripts.create__purchase_notification import create_purchase_notification

LOGGER = logging.getLogger(__name__)


class PurchasePublisher:
    RECONNECT_DELAY = 5

    def __init__(self, amqp_url, queue_name, routing_key, exchange=''):
        self._connection = None
        self._channel = None
        self._deliveries = {}
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._url = amqp_url
        self.queue = queue_name
        self.routing_key = routing_key
        self.exchange = exchange
        self._stopping = False

    def run(self):
        while not self._stopping:
            self._connection = None
            self._channel = None
            self._acked = 0
            self._nacked = 0
            self._message_number = 0

            try:
                self._connection = self.connect()
                self._connection.ioloop.start()
            except KeyboardInterrupt:
                self.stop()
                if (self._connection is not None and
                        not self._connection.is_closed):
                    self._connection.ioloop.start()
                break
            except Exception as e:
                LOGGER.error("Main loop crashed: %s", e)

        LOGGER.info('Publisher loop finished.')

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
            LOGGER.warning('Connection closed, reopening in %i seconds: %s',self.RECONNECT_DELAY,
                           reason)
            self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_channel_open(self, channel):
        LOGGER.info(f"Channel opened")
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self._channel.queue_declare(
            queue=self.queue,
            durable=True,
            callback=self.on_queue_declared
        )

    def on_channel_closed(self, channel, reason):
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self._channel = None
        if not self._stopping:
            self._connection.close()

    def on_queue_declared(self, _unused_frame):
        LOGGER.info("Queue ready. Enabling publisher confirms.")
        self._channel.confirm_delivery(ack_nack_callback=self.on_delivery_confirmation)
        if self._deliveries:
            self.resend_pending_messages()
        self.schedule_next_message()

    def on_delivery_confirmation(self, frame):
        confirmation_type = frame.method.NAME.split('.')[1].lower()
        ack_multiple = frame.method.multiple
        delivery_tag = frame.method.delivery_tag
        LOGGER.info('Received %s for delivery tag: %i (multiple: %s)',
                    confirmation_type, delivery_tag, ack_multiple)
        if isinstance(frame.method, pika.spec.Basic.Ack):
            self._acked += 1
        elif isinstance(frame.method, pika.spec.Basic.Nack):
            self._nacked += 1

        del self._deliveries[delivery_tag]

        if ack_multiple:
            for tmp_tag in list(self._deliveries.keys()):
                if tmp_tag <= delivery_tag:
                    self._acked += 1
                    del self._deliveries[tmp_tag]
        LOGGER.info(
            'Published %i messages, %i have yet to be confirmed, '
            '%i were acked and %i were nacked', self._message_number,
            len(self._deliveries), self._acked, self._nacked)

    def schedule_next_message(self):
        wait_time = random.randint(1, 6)
        LOGGER.info(f"Scheduling next publish in {wait_time}s...")
        self._connection.ioloop.call_later(wait_time, self.publish_message)

    def publish_message(self):
        if self._channel is None or not self._channel.is_open:
            return

        new_purchase = create_purchase_notification()
        payload = json.dumps(asdict(new_purchase), default=lambda o: o.isoformat())
        self._channel.basic_publish(self.exchange, self.routing_key,
                                    body=payload,
                                    properties=pika.BasicProperties(
                                        content_type="application/json",
                                        delivery_mode=pika.DeliveryMode.Persistent
                                    ))

        self._message_number += 1
        self._deliveries[self._message_number] = payload
        LOGGER.info('Published message # %i', self._message_number)
        self.schedule_next_message()

    def resend_pending_messages(self):
        LOGGER.info('Resending %i pending messages', len(self._deliveries))
        for tag in sorted(self._deliveries.keys()):
            self._channel.basic_publish(self.exchange, self.routing_key,
                                        body=self._deliveries[tag],
                                        properties=pika.BasicProperties(
                                            content_type="application/json",
                                            delivery_mode=pika.DeliveryMode.Persistent
                                        ))
        self._deliveries.clear()
        self._message_number = 0

    def stop(self):
        LOGGER.info('Stopping...')
        self._stopping = True
        self._channel.close()
        self._connection.close()
