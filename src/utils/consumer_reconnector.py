import logging
import time

LOGGER = logging.getLogger(__name__)


class ReconnectingConsumer:

    def __init__(self, consumer_class, **consumer_kwargs):
        self._reconnect_delay = 0
        self._consumer_class = consumer_class
        self._consumer_kwargs = consumer_kwargs
        self._consumer = self._consumer_class(**self._consumer_kwargs)

    def run(self):
        while True:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.info('Reconnecting after %d seconds', reconnect_delay)
            time.sleep(reconnect_delay)
            self._consumer = self._consumer_class(**self._consumer_kwargs)

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
            if self._reconnect_delay > 30:
                self._reconnect_delay = 30
        return self._reconnect_delay
