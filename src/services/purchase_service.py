class PurchaseService:
    def __init__(self, redis_repo, kafka_producer):
        self.redis = redis_repo
        self.kafka = kafka_producer

    def process(self, notification:dict, raw_body)->bool:
        success = self.redis.add_notification(notification)
        if success:
            success = self.kafka.publish_notification('purchase_topic', notification['order_id'], raw_body)
            if success:
                return True
            return False
        return False

    def close_connections(self)->None:
        self.redis.close_connection()
        self.kafka.close_connection()