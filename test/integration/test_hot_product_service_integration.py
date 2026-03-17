import pytest

from src.infrastructure.database.redis_client_creator import create_redis_client
from src.infrastructure.repositories.product.mock_product_repository import MockProductRepository
from src.infrastructure.repositories.purchase.redis_purchase_repository import RedisPurchaseRepository
from src.services.hot_product_service import HotProductService


@pytest.fixture
def redis_client():
    client = create_redis_client(db=1)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()



@pytest.fixture
def test_process_integration(redis_repo):
    pass
