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


def test_process_invalid_json(service, mock_redis_repo):
    bad_notification = "this is not a json"
    result = service.process(bad_notification)
    assert result is True
    mock_redis_repo.increment_product_count.assert_not_called()


def test_get_top_products(service: HotProductService, mock_redis_repo: MagicMock):
    mock_redis_repo.get_hot_products.side_effect = [[], [('1', 100.0)]]
    result = service.get_top_products(count=3)
    assert len(result) == 1
    assert result[0]['product_id'] == 1
    assert result[0]['current_score'] == 100
    assert mock_redis_repo.get_hot_products.call_count == 2


def test_process_redis_failure(service, mock_redis_repo):
    notification = json.dumps({"product_id": 1, "quantity": 1})
    mock_redis_repo.increment_product_count.return_value = False
    result = service.process(notification)
    assert result is False
    mock_redis_repo.increment_product_count.assert_called_once()

