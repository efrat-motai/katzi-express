from src.infrastructure.messaging.consumers.kafka_consumer import KafkaConsumer
from src.setup import bootstrap_service
from src.utils.config.config_loader import load_config
from src.utils.types.redis_profile import RedisProfile


def main():
    config = load_config()
    hot_product_service = bootstrap_service(profile= RedisProfile.CONSUMER)
    if hot_product_service is None:
        return
    kafka_config = config["kafka_consumer"]
    consumer = KafkaConsumer(hot_product_service)
    consumer.connect(kafka_config, ['purchase_topic'])
    consumer.consume_notification()


if __name__ == '__main__':
    main()
