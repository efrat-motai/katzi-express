import redis
import logging
from redis.backoff import ExponentialBackoff
from redis.retry import Retry

LOGGER = logging.getLogger(__name__)


def create_redis_client(host="localhost", port=6379, db=0):
    retry = Retry(ExponentialBackoff(), 10)
    client = redis.Redis(retry=retry, host=host, port=port, db=db, decode_responses=True)
    client.ping()
    LOGGER.info(f"Successfully connected to Redis at {host}:{port}")
    return client
