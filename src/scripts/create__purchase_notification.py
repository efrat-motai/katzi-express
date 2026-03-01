from datetime import datetime
import uuid
from src.models.purchase_notification import PurchaseNotification
from src.data.mock_data import products, customers
import random
import time


def create_purchase_notification():
    while True:
        time_to_sleep = random.randint(1, 6)
        product_index = random.randint(0, len(products) - 1)
        customer_index = random.randint(0, len(customers) - 1)
        quantity = random.randint(1, 10)
        product = products[product_index]
        customer = customers[customer_index]
        new_purchase = PurchaseNotification(product_id=product["product_id"], product_name=product["product_name"],
                                            product_category=product["product_category"], price=product["price"],
                                            customer_id=customer["customer_id"], quantity=quantity,
                                            order_date=datetime.now(), order_id=uuid.uuid1())
        print(new_purchase)
        time.sleep(time_to_sleep)


create_purchase_notification()
