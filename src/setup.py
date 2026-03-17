import logging

from src.infrastructure.database.redis_client_creator import create_redis_client
from src.infrastructure.repositories.product.mock_product_repository import MockProductRepository
from src.infrastructure.repositories.purchase.redis_purchase_repository import RedisPurchaseRepository
from src.services.hot_product_service import HotProductService
from src.utils.config_loader import load_config

LOGGER = logging.getLogger(__name__)


def bootstrap_service() -> HotProductService | None:
    config = load_config()
    redis_config = config["redis"]

    try:
        redis_client = create_redis_client(
            host=redis_config["host"],
            port=redis_config["port"],
            db=0
        )
    except Exception as e:
        LOGGER.critical(f"Could not connect to Redis. Application exiting. Error: {e}")
        return None

    redis = RedisPurchaseRepository(ttl=redis_config["ttl"], redis_client=redis_client)
    hot_product_service = HotProductService(redis, MockProductRepository())
    return hot_product_service
