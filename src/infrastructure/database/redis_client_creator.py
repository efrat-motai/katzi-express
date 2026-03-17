import redis
import logging

from retry import retry

LOGGER = logging.getLogger(__name__)


@retry(tries=5, delay=2, backoff=2, exceptions=redis.ConnectionError)
def create_redis_client(host="localhost", port=6379, db=0):
        client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        client.ping()
        LOGGER.info(f"Successfully connected to Redis at {host}:{port}")
        return client
