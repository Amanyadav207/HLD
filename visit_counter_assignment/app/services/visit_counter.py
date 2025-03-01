import asyncio
import time
import hashlib
from ..core.redis_manager import RedisManager

class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with Redis manager and in-memory cache"""
        # Initialize Redis managers for each shard
        self.redis_shards = {
            'redis_7070': RedisManager(port=7070),
            'redis_7071': RedisManager(port=7071)
        }
        self.cache = {} 
        self.ttl = 5 
        self.buffer = {}  # In-memory buffer for batching
        self.flush_interval = 30  # Flush interval in seconds
        asyncio.create_task(self._flush_buffer_periodically())

    def _get_shard(self, page_id: str):
        """
        Determine which Redis shard to use based on consistent hashing.
        """
        hash_value = int(hashlib.md5(page_id.encode()).hexdigest(), 16)
        shard_key = 'redis_7070' if hash_value % 2 == 0 else 'redis_7071'
        return self.redis_shards[shard_key], shard_key

    async def increment_visit(self, page_id: str) -> int:
        """
        Increment visit count for a page and store in buffer.
        """
        self.buffer[page_id] = self.buffer.get(page_id, 0) + 1
        return self.buffer[page_id]

    async def _flush_buffer_periodically(self):
        """
        Periodically flush the buffer to Redis.
        """
        while True:
            await asyncio.sleep(self.flush_interval)
            await self.flush_buffer()

    async def flush_buffer(self):
        """
        Flush the in-memory buffer to Redis.
        """
        for page_id, count in self.buffer.items():
            try:
                shard, shard_key = self._get_shard(page_id)
                await shard.increment(page_id, count)
            except Exception as e:
                print(f"Error flushing buffer for {page_id} to {shard_key}: {e}")
        self.buffer.clear()

    async def get_visit_count(self, page_id: str) -> dict:
        """
        Get the current visit count for a page.
        Combine the count from Redis with the buffer.
        """
        # Check if the value exists in cache and is still valid
        if page_id in self.cache:
            value, timestamp = self.cache[page_id]
            if time.time() - timestamp < self.ttl:
                return {"visits": value + self.buffer.get(page_id, 0), "served_via": "in_memory"}

        # If not in cache or expired, query Redis
        try:
            shard, shard_key = self._get_shard(page_id)
            count = await shard.get(page_id)
            count = count if count is not None else 0
            total_count = count + self.buffer.get(page_id, 0)
            self.cache[page_id] = (total_count, time.time())  # Update cache
            return {"visits": total_count, "served_via": shard_key}
        except Exception as e:
            print(f"Error retrieving visit count for {page_id}: {e}")
            return {"visits": 0, "served_via": "error"}
