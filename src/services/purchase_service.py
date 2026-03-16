class PurchaseService:
    def __init__(self, kafka_producer):
        self.kafka = kafka_producer

    def process(self, notification:dict, raw_body)->bool:
        success = self.kafka.publish_notification('purchase_topic', notification['order_id'], raw_body)
        if success:
            return True
        return False

    def close_connections(self)->None:
        self.kafka.close_connection()