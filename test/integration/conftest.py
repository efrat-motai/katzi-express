import pytest
from fastapi.testclient import TestClient

from src.api.hot_products_api import app
from src.infrastructure.database.redis_client_creator import create_redis_client
from src.infrastructure.repositories.product.mock_product_repository import MockProductRepository
from src.infrastructure.repositories.hot_products.hot_products_tumbling_store import HotProductsTumblingStore
from src.services.hot_products_tumbling_service import HotProductsTumblingService



@pytest.fixture
def redis_client():
    client = create_redis_client(db=1)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def integration_service(redis_client):
    redis_repository = HotProductsTumblingStore(redis_client=redis_client, ttl=60)
    product_repository = MockProductRepository()
    return HotProductsTumblingService(redis_repo=redis_repository, product_repo=product_repository)

@pytest.fixture
def api_client():
    return TestClient(app)