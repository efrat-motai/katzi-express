import json
from unittest.mock import MagicMock, patch
from src.services.hot_product_service import HotProductService


def test_process_success(service: HotProductService, mock_redis_repo: MagicMock):
    purchase_notification = json.dumps({"product_id": 1, "quantity": 1})
    mock_redis_repo.increment_product_count.return_value = True

    with patch('src.services.hot_product_service.time.time', return_value=1773745199.3122442):
        result = service.process(purchase_notification)

    assert result == True
    expected_key = 'hot_products:1773745140'
    mock_redis_repo.increment_product_count.assert_called_once_with(key=expected_key, product_id=1, amount=1)


def test_process_invalid_json(service, mock_redis_repo):
    bad_notification = "this is not a json"
    result = service.process(bad_notification)
    assert result is True
    mock_redis_repo.increment_product_count.assert_not_called()


def test_get_top_products(service: HotProductService, mock_redis_repo: MagicMock, mock_products_repo: MagicMock):
    mock_redis_repo.get_hot_products.side_effect = [[], [('1', 100.0)]]
    mock_products_repo.get_by_id.return_value = {"product_id": 1, "name": "Test Product"}
    result = service.get_top_products(count=3)
    assert len(result) == 1
    assert result[0]['product_id'] == 1
    assert result[0]['name'] == 'Test Product'
    assert result[0]['current_score'] == 100
    assert mock_redis_repo.get_hot_products.call_count == 2


def test_process_redis_failure(service, mock_redis_repo):
    notification = json.dumps({"product_id": 1, "quantity": 1})
    mock_redis_repo.increment_product_count.return_value = False
    result = service.process(notification)
    assert result is False
    mock_redis_repo.increment_product_count.assert_called_once()
