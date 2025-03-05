# \\routers\stock.py
from datetime import date
from fastapi import APIRouter, Depends, Query
from fastapi.security import HTTPBearer
from sqlmodel.ext.asyncio.session import AsyncSession

from config.database import AsyncSessionLocal
from config.get_db import get_db
from user_module.services.stock_service import StockService
from utils.response_util import ResponseUtil

stockhistrouter = APIRouter(
    prefix="/api/stock",
    tags=["股票行情"],
    dependencies=[Depends(HTTPBearer())]
)


@stockhistrouter.get("/kline", summary="获取K线数据")
async def get_kline(

        symbol: str = Query(
            ...,
            min_length=6,
            example="600000",
            regex=r"^(\d{6})(\.(SH|SZ))?$"  # 允许 600000 或 600000.SH
        ),
        # 统一参数别名格式为小写下划线（与前端一致）
        adjust_type: str = Query(
            "qfq",
            alias="adjust_type",  # 修改别名
            enum=["qfq", "hfq", "normal"]
        ),
        # 使用 date 类型接收日期
        start_date: date = Query(
            None,
            alias="start_date",
            example="2024-03-01",
            description="开始日期 (格式: YYYY-MM-DD)"
        ),
        end_date: date = Query(
            None,
            alias="end_date",
            example="2024-03-15",
            description="结束日期 (格式: YYYY-MM-DD)"
        ),
        # session: AsyncSession = Depends(AsyncSessionLocal)
        query_db: AsyncSession = Depends(get_db)
):
    # # 自动补全股票代码后缀
    # if '.' not in symbol:
    #     symbol = f"{symbol}.SH"
    query_result =  await StockService.get_kline(
        query_db,
        symbol.upper(),
        adjust_type,
        start_date,
        end_date
    )
    print(query_result)
    return ResponseUtil.success(data=query_result)