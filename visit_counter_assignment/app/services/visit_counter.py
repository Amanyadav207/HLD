import asyncio
import time
from ..core.redis_manager import RedisManager

class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with Redis manager and in-memory cache"""
        self.redis_manager = RedisManager()
        self.cache = {} 
        self.ttl = 5 

    async def increment_visit(self, page_id: str) -> int:
        """
        Increment visit count for a page and store in Redis.
        Also updates in-memory cache.
        """
        try:
            new_count = await self.redis_manager.increment(page_id)
            self.cache[page_id] = (new_count, time.time())  # Update cache with timestamp
            return new_count
        except Exception as e:
            print(f"Error incrementing visit count: {e}")
            return 0

    async def get_visit_count(self, page_id: str) -> dict:
        """
        Get the current visit count for a page.
        First, check the in-memory cache. If expired or not found, query Redis.
        """
        # Check if the value exists in cache and is still valid
        if page_id in self.cache:
            value, timestamp = self.cache[page_id]
            if time.time() - timestamp < self.ttl:
                return {"visits": value, "served_via": "in_memory"}

        # If not in cache or expired, query Redis
        try:
            count = await self.redis_manager.get(page_id)
            count = count if count is not None else 0
            self.cache[page_id] = (count, time.time())  # Update cache
            return {"visits": count, "served_via": "redis"}
        except Exception as e:
            print(f"Error retrieving visit count: {e}")
            return {"visits": 0, "served_via": "error"}
