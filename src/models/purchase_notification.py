import datetime
from dataclasses import dataclass


@dataclass
class PurchaseNotification:
    product_id: int
    product_name: str
    product_category: str
    price: float
    quantity: int
    customer_id: int
    order_date: datetime
    order_id: int


