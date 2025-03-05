from datetime import datetime
from pydantic import BaseModel

from user_module.schemas.response import Response200


class StockKlineBase(BaseModel):
    symbol: str
    date: datetime
    open: float
    close: float
    high: float
    low: float

class StockKlineResponse(Response200):
    data: list[StockKlineBase]
    metadata: dict = {
        "example": {
            "total": 100,
            "page": 1,
            "page_size": 100
        }
    }