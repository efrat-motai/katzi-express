from dataclasses import dataclass
from typing import Optional, Any
from redis.backoff import ExponentialBackoff
from redis.retry import Retry


@dataclass
class RedisSettings:
    socket_timeout: float
    socket_connect_timeout: float
    retry_on_timeout: bool
    retry: Optional[Retry] = None

    @classmethod
    def from_config(cls, profile_data: dict[str, Any]) -> "RedisSettings":
        retry_on_timeout = profile_data.get("retry_on_timeout", False)
        retry = None
        if retry_on_timeout:
            attempt = profile_data.get("retry_attempts", 5)
            retry = Retry(ExponentialBackoff(), attempt)
        socket_connect_timeout = profile_data.get("socket_connect_timeout", 1.0)
        socket_timeout = profile_data.get("socket_timeout", 0.5)
        return cls(
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            retry_on_timeout=retry_on_timeout,
            retry=retry,
        )
