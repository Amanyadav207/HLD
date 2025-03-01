from fastapi import APIRouter, HTTPException
from ....services.visit_counter import VisitCounterService
from ....schemas.counter import VisitCount

router = APIRouter()
counter_service = VisitCounterService()

@router.post("/visit/{page_id}")
async def increment_visit(page_id: str):
    """Record a visit for a website"""
    try:
        count = counter_service.increment_visit(page_id)
        return {"visits": count, "served_via": "in_memory"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/visits/{page_id}")
async def get_visit_count(page_id: str):
    """Get visit count for a website"""
    try:
        result = counter_service.get_visit_count(page_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))