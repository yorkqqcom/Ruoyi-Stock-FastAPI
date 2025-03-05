# \\service\stock_service.py
from datetime import date
from sqlmodel import select
from sqlalchemy import between
from sqlmodel.ext.asyncio.session import AsyncSession

from user_module.models.stock import StockHist

# 正确代码：直接使用传入的 AsyncSession 实例
class StockService:
    @staticmethod
    async def get_kline(
        query_db: AsyncSession,  # ✅ 接收真实的会话实例
        symbol: str,
        adjust_type: str = "qfq",
        start_date: date = None,
        end_date: date = None,
        page: int = 1,
        page_size: int = 500
    ):
        # 无需创建会话，直接使用传入的 session
        query = (
            select(StockHist)
            .where(StockHist.symbol == symbol)
            .where(StockHist.adjust_type == adjust_type)
            .order_by(StockHist.date)
        )

        if start_date and end_date:
            query = query.where(between(StockHist.date,start_date, end_date))

        paginated_query = query.offset((page - 1) * page_size).limit(page_size)
        # result = await query_db.exec(paginated_query)
        result = await query_db.execute(paginated_query)

        # return result.scalars().all()
        return result.scalars().all()