import pika
import json
import logging
import random
from dataclasses import asdict
from pika.exchange_type import ExchangeType
from src.scripts.create__purchase_notification import create_purchase_notification

logging.basicConfig(level=logging.INFO)


# LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
#               '-35s %(lineno) -5d: %(message)s')
# LOGGER = logging.getLogger(__name__)


class PurchasePublisher:
    EXCHANGE = ''
    EXCHANGE_TYPE = ExchangeType.topic
    PUBLISH_INTERVAL = 1
    QUEUE = 'purchase_notifications'
    ROUTING_KEY = 'purchase_notifications'

    def __init__(self):
        self.confirmed = 0
        self.errors = 0
        self.published = 0
        self.parameters = pika.ConnectionParameters('localhost')
        self._connection = None
        self._channel = None

    def run(self):
        self._connection = pika.SelectConnection(self.parameters,
                                                 on_open_callback=self.on_connection_open,
                                                 on_open_error_callback=self.on_connection_failed,
                                                 on_close_callback=self.on_connection_close)
        self._connection.ioloop.start()

    def on_connection_open(self, _unused_connection):
        logging.info("Connection opened")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_connection_failed(self, conn, error):
        logging.info(f"Connection failed : {error}")
        self._connection.ioloop.stop()

    def on_connection_close(self, conn, reason):
        logging.info(f"Connection closed: {reason}")
        self._connection.ioloop.stop()

    def on_channel_open(self, channel):
        logging.info(f"Channel opened")
        self._channel = channel
        self._channel.queue_declare(
            queue=self.QUEUE,
            durable=True,
            callback=self.on_queue_declared
        )

    def on_queue_declared(self, _unused_frame):
        logging.info("Queue ready. Enabling publisher confirms.")
        self._channel.confirm_delivery(ack_nack_callback=self.on_delivery_confirmation)
        self.schedule_next_message()

    def on_delivery_confirmation(self, frame):
        if isinstance(frame.method, pika.spec.Basic.Ack):
            self.confirmed += 1
            logging.info('Received confirmation: %r', frame.method)
        else:
            logging.error('Received negative confirmation: %r', frame.method)
            self.errors += 1

    def schedule_next_message(self):
        wait_time = random.randint(1, 6)
        logging.info(f"Scheduling next publish in {wait_time}s...")
        self._connection.ioloop.call_later(wait_time, self.publish_message)

    def publish_message(self):
        if self._channel is None or not self._channel.is_open:
            return

        new_purchase = create_purchase_notification()
        self._channel.basic_publish(self.EXCHANGE, self.ROUTING_KEY,
                                    body=json.dumps(asdict(new_purchase), default=lambda o: o.isoformat()),
                                    properties=pika.BasicProperties(
                                        content_type="application/json",
                                        delivery_mode=pika.DeliveryMode.Persistent
                                    ))

        self.published += 1
        self.schedule_next_message()


if __name__ == "__main__":

    publisher = PurchasePublisher()
    try:
        publisher.run()
    except KeyboardInterrupt:
        pass
