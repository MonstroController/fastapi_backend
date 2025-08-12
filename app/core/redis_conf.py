import redis
from functools import lru_cache

@lru_cache(maxsize=1)
def get_redis_client() -> redis.Redis:
    return redis.Redis(host="broker_redis", port=6379, db=3)