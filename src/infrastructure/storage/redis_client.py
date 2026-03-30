import redis
import logging
from src.models.redis_settings import RedisSettings
from typing import Any

LOGGER = logging.getLogger(__name__)


def get_redis_client(host="localhost", port=6379, db=0, profile_data: dict[str, Any] = None):
    if profile_data is None:
        profile_data = {}
    redis_settings =RedisSettings.from_config(profile_data=profile_data)
    client = redis.Redis(
        host=host,
        port=port,
        db=db,
        decode_responses=True,
        socket_timeout=redis_settings.socket_timeout,
        socket_connect_timeout=redis_settings.socket_connect_timeout,
        retry=redis_settings.retry,
        retry_on_timeout=redis_settings.retry_on_timeout,
    )

    return client
