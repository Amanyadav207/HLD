from typing import Dict, List, Any
import asyncio
from datetime import datetime
from ..core.redis_manager import RedisManager
from ..schemas.counter import VisitCount

class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with Redis manager"""
        # self.redis_manager = RedisManager()
        self.map: Dict[str, int] = {}

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
        """
        # TODO: Implement visit count increment

        if page_id in self.map:
            print("present")
            self.map[page_id] += 1
        else:
            print("absent")
            self.map[page_id] = 1
        print("counter", self.map[page_id])

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
        # TODO: Implement getting visit count
        print("hi")
        return self.map.get(page_id,0)
