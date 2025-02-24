from typing import Any
from ..core.redis_manager import RedisManager

class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with Redis manager"""
        self.redis_manager = RedisManager()  # Use Redis instead of in-memory storage

    async def increment_visit(self, page_id: str) -> int:
        """
        Increment visit count for a page and store in Redis
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            New visit count
        """
        try:
            new_count = await self.redis_manager.increment(page_id)
            return new_count
        except Exception as e:
            print(f"Error incrementing visit count: {e}")
            return 0  # Return 0 in case of failure

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page from Redis
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
        try:
            count = await self.redis_manager.get(page_id)
            return count if count is not None else 0  # Return 0 if key doesn't exist
        except Exception as e:
            print(f"Error retrieving visit count: {e}")
            return 0