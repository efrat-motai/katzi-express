import logging

import yaml

from src.infrastructure.consumers.kafka_consumer import KafkaConsumer
from src.infrastructure.repositories.redis_repository import RedisRepository
from src.services.hot_product_service import HotProductService


def main():
    config = yaml.safe_load(open("../config/config.yml"))
    logging.basicConfig(level=config["logging"]["level"], format=config["logging"]["format"])
    logging.getLogger('pika').setLevel(logging.WARNING)

    redis_host = config["redis"]["host"]
    redis_port = config["redis"]["port"]
    ttl = config["redis"]["ttl"]
    redis = RedisRepository(redis_host, redis_port, ttl)

    hot_product_service = HotProductService(redis)

    kafka_config = config["kafka_consumer"]
    consumer = KafkaConsumer(hot_product_service)
    consumer.connect(kafka_config, ['purchase_topic'])
    consumer.consume_notification()

if __name__ == '__main__':
    main()