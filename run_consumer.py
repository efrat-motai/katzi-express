import logging
import yaml
from src.base_reconnector import ReconnectingConsumer
from src.consumers.purchase_consumer import PurchaseConsumer
from src.models.redis_repository import RedisRepository
from src.publishers.kafka_producer import KafkaRepository


def main():
    config = yaml.safe_load(open("config.yml"))
    logging.basicConfig(level=config["logging"]["level"], format=config["logging"]["format"])
    logging.getLogger('pika').setLevel(logging.WARNING)
    amqp_url = config["rabbitmq"]["url"]
    queue_name = config["rabbitmq"]["queue"]

    def create_redis():
        redis_host = config["redis"]["host"]
        redis_port = config["redis"]["port"]
        return RedisRepository(redis_host, redis_port)

    def create_kafka_producer():
        kafka_config = config["kafka"]
        return KafkaRepository(kafka_config)

    consumer_params = {
        'amqp_url': amqp_url,
        'queue_name': queue_name,
        'exchange': 'purchases_exchange',
        'redis_factory': create_redis,
        'kafka_factory': create_kafka_producer
    }
    consumer = ReconnectingConsumer(PurchaseConsumer, **consumer_params)
    consumer.run()


if __name__ == '__main__':
    main()
