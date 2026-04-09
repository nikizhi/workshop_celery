import redis.asyncio as redis

from app.core.settings import settings

redis_client = redis.from_url(
    url=settings.redis_url,
    decode_responses=True
)