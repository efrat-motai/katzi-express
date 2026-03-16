from src.infrastructure.messaging.consumers.kafka_consumer import KafkaConsumer
from src.infrastructure.repositories.redis_purchase_repository import RedisPurchaseRepository
from src.services.hot_product_service import HotProductService
from src.utils.config_loader import load_config


def main():
    config = load_config()
    redis_config = config["redis"]
    redis = RedisPurchaseRepository(host=redis_config["host"], port=redis_config["port"], ttl=redis_config["ttl"])

    hot_product_service = HotProductService(redis)

    kafka_config = config["kafka_consumer"]
    consumer = KafkaConsumer(hot_product_service)
    consumer.connect(kafka_config, ['purchase_topic'])
    consumer.consume_notification()


if __name__ == '__main__':
    main()
