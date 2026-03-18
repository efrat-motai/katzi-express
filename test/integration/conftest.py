import pytest
from fastapi.testclient import TestClient

from src.api.hot_products_api import app
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
def integration_service(redis_client):
    redis_repository = RedisPurchaseRepository(redis_client=redis_client, ttl=60)
    product_repository = MockProductRepository()
    return HotProductService(redis_repo=redis_repository, product_repo=product_repository)

@pytest.fixture
def api_client():
    return TestClient(app)