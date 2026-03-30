import json
import pytest
from unittest.mock import MagicMock
from src.services.hot_products_tumbling_service import HotProductsTumblingService


def test_handle_purchase_event_align_to_window_start(hot_product_service: HotProductsTumblingService, mock_redis_repo: MagicMock):
    event_time = "2026-03-24T00:33:12.228445"
    expected_window_ts = 1774305180
    payload = json.dumps({
        "product_id": 1,
        "quantity": 1,
        "purchase_timestamp": event_time})
    mock_redis_repo.increment_product_count.return_value = True
    result = hot_product_service.handle_purchase_event(payload)
    assert result == True
    expected_key = f'hot_products:{expected_window_ts}'
    mock_redis_repo.increment_product_count.assert_called_once_with(key=expected_key, product_id=1, amount=1)


def test_handle_purchase_missing_required_fields_skips_processing(hot_product_service: HotProductsTumblingService, mock_redis_repo: MagicMock):
    bad_notification = json.dumps({"product": 1, "price": 1})
    result = hot_product_service.handle_purchase_event(bad_notification)
    assert result is True
    mock_redis_repo.increment_product_count.assert_not_called()


def test_handle_purchase_malformed_json_skips_processing(hot_product_service, mock_redis_repo: MagicMock):
    bad_notification = "this is not a json"
    result = hot_product_service.handle_purchase_event(bad_notification)
    assert result is True
    mock_redis_repo.increment_product_count.assert_not_called()


@pytest.mark.parametrize("input_ts,expected_ts",
                         [(1774225500.1, 1774225500), (1774225559.99, 1774225500), (1774225560, 1774225560)])
def test_calculate_window_start(hot_product_service, input_ts: float, expected_ts: float):
    assert hot_product_service._calculate_window_start(input_ts) == expected_ts


def test_get_top_products_returns_product_details_from_multiple_window(hot_product_service: HotProductsTumblingService, mock_redis_repo: MagicMock,
                                                                       mock_products_repo: MagicMock):
    mock_redis_repo.get_hot_products.side_effect = [[], [('1', 100.0)]]
    mock_products_repo.get_by_id.return_value = {"product_id": 1, "name": "Test Product"}
    result = hot_product_service.get_top_products(count=3)
    assert len(result) == 1
    assert result[0]['product_id'] == 1
    assert result[0]['name'] == 'Test Product'
    assert result[0]['current_score'] == 100
    assert mock_redis_repo.get_hot_products.call_count == 2


def test_handle_purchase_redis_failure(hot_product_service, mock_redis_repo: MagicMock):
    notification = json.dumps({"product_id": 1, "quantity": 1, "purchase_timestamp": "2026-03-24T00:33:12.228445"})
    mock_redis_repo.increment_product_count.return_value = False
    result = hot_product_service.handle_purchase_event(notification)
    assert result is False
    mock_redis_repo.increment_product_count.assert_called_once()
