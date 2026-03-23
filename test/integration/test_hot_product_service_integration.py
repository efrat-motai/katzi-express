import json


def test_process_integration(integration_service, redis_client):
    notification = json.dumps({"product_id": 1, "quantity": 10})
    success = integration_service.handle_purchase_event(notification)
    assert success

    keys = redis_client.keys("hot_products:*")
    assert len(keys) == 1

    score = redis_client.zscore(keys[0], "1")
    assert score == 10

    top_list = integration_service.get_top_products(count=1)

    assert len(top_list) == 1
    assert top_list[0]['product_id'] == 1
    assert top_list[0]['current_score'] == 10
