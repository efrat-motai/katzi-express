import json
import pytest
from unittest.mock import MagicMock, patch

from src.infrastructure.repositories.redis_purchase_repository import RedisPurchaseRepository
from src.services.hot_product_service import HotProductService


@pytest.fixture
def mock_redis_repo():
    return MagicMock(spec=RedisPurchaseRepository)

@pytest.fixture
def service(mock_redis_repo):
    return HotProductService(redis_repo=mock_redis_repo)

def test_process_success(service: HotProductService, mock_redis_repo: MagicMock):
    purchase_notification = json.dumps({"product_id": 1, "quantity": 1})
    mock_redis_repo.increment_product_count.return_value = True

    with patch('src.services.hot_product_service.time.time', return_value=1773745199.3122442):
        result = service.process(purchase_notification)

    assert result == True
    expected_key = 'hot_products:1773745140'
    mock_redis_repo.increment_product_count.assert_called_once_with(expected_key, 1)