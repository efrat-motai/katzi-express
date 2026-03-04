import logging
import yaml
from src.publishers.purchase_publisher import PurchasePublisher


def main():
    config = yaml.safe_load(open("config.yml"))
    logging.basicConfig(level=config["logging"]["level"], format=config["logging"]["format"])
    logging.getLogger('pika').setLevel(logging.WARNING)
    amqp_url = config["rabbitmq"]["url"]
    queue_name = config["rabbitmq"]["queue"]
    publisher = PurchasePublisher(
        amqp_url=amqp_url,
        queue_name=queue_name,
        exchange='purchases_exchange'
    )
    publisher.run()

if __name__ == '__main__':
    main()
