class PurchaseService:
    def __init__(self, redis_repo, kafka_producer):
        self.redis = redis_repo
        self.kafka = kafka_producer

    def process(self, notification_dict, raw_body):
        success = self.redis.add_notification(notification_dict)
        if success:
            self.kafka.publish_notification('purchase_topic', notification_dict['order_id'], raw_body)
            return True
        return False