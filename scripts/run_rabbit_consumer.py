from pika.exchange_type import ExchangeType
from src.services.purchase_service import PurchaseService
from src.utils.config_loader import load_config
from src.infrastructure.messaging.consumers.rabbit_consumer import PurchaseConsumer
from src.infrastructure.messaging.producers.kafka_producer import KafkaProducer


def main():
    config = load_config()

    amqp_url = config["rabbitmq"]["url"]
    queue_name = config["rabbitmq"]["queue"]

    kafka_config = config["kafka_producer"]
    kafka = KafkaProducer(kafka_config)

    consumer_params = {
        'amqp_url': amqp_url,
        'queue_name': queue_name,
        'exchange': 'purchases_exchange',
        'routing_key': queue_name,
        'exchange_type': ExchangeType.direct,
        'data_service': PurchaseService(kafka)

    }
    consumer = PurchaseConsumer(**consumer_params)
    consumer.run()


if __name__ == '__main__':
    main()
