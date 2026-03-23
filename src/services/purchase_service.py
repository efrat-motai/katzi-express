class PurchaseService:
    def __init__(self, kafka_producer):
        self.kafka = kafka_producer

    def dispatch_to_kafka(self, notification: dict, raw_body) -> bool:
        return self.kafka.publish_notification('purchase_topic', notification['product_id'], raw_body)

    def close_connections(self) -> None:
        self.kafka.close_connection()
