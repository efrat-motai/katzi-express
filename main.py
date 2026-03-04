import logging
from src.scripts.purchase_publisher import PurchasePublisher
import yaml


def main():
    config = yaml.safe_load(open("config.yml"))
    logging.basicConfig(level=config["logging"]["level"], format=config["logging"]["format"])
    logging.getLogger('pika').setLevel(logging.WARNING)
    publisher = PurchasePublisher(
        amqp_url=config['rabbitmq']['url'],
        queue_name=config['rabbitmq']['queue'],
    )
    publisher.run()


if __name__ == '__main__':
    main()
