import logging
import yaml
from src.base_reconnector import ReconnectingConsumer
from src.consumers.purchase_consumer import PurchaseConsumer


def main():
    config = yaml.safe_load(open("config.yml"))
    logging.basicConfig(level=config["logging"]["level"], format=config["logging"]["format"])
    logging.getLogger('pika').setLevel(logging.WARNING)
    amqp_url = config["rabbitmq"]["url"]
    queue_name = config["rabbitmq"]["queue"]
    consumer_config = {
        'amqp_url': amqp_url,
        'queue_name': queue_name,
        'exchange': 'purchases_exchange'
    }
    consumer = ReconnectingConsumer(PurchaseConsumer, **consumer_config)
    consumer.run()

if __name__ == '__main__':
    main()
