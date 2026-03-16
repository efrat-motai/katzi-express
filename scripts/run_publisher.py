from pika.exchange_type import ExchangeType
from src.generators.purchase_notification_generator import generate_purchase_notification
from src.infrastructure.producers.rabbit_producer import RabbitProducer
from src.utils.config_loader import load_config


def main():
    config = load_config()

    amqp_url = config["rabbitmq"]["url"]
    queue_name = config["rabbitmq"]["queue"]
    publisher = RabbitProducer(
        amqp_url=amqp_url,
        queue_name=queue_name,
        exchange='purchases_exchange',
        exchange_type=ExchangeType.direct,
        routing_key=queue_name,
        generator_func= generate_purchase_notification
    )
    publisher.run()

if __name__ == '__main__':
    main()
