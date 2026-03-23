from datetime import datetime
import uuid
from src.models.purchase_notification import PurchaseNotification
from src.utils.data.mock_data import PRODUCTS, CUSTOMERS
import random


def generate_purchase_notification()->PurchaseNotification:
    weights = [p.get("popularity", 1) for p in PRODUCTS]
    product = random.choices(PRODUCTS, weights=weights, k=1)[0]
    customer_index = random.randint(0, len(CUSTOMERS) - 1)
    quantity = random.randint(1, 10)
    customer = CUSTOMERS[customer_index]
    new_purchase = PurchaseNotification(product_id=product["product_id"], product_name=product["product_name"],
                                        product_category=product["product_category"], price=product["price"],
                                        customer_id=customer["customer_id"], quantity=quantity,
                                        purchase_timestamp=datetime.now(), purchase_id=str(uuid.uuid1()))
    return new_purchase
