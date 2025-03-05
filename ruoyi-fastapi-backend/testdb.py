# 异步测试代码（例如在main.py中）
import asyncio
from datetime import datetime
from config.database import async_engine, AsyncSessionLocal, Base
from user_models.stock.stock_hist import StockHist

async def create_tables():
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)  # 异步创建表

async def insert_test_data():
    async with AsyncSessionLocal() as session:
        test_record = StockHist(
            date=datetime.now(),
            symbol="AAPL",
            adjust_type="none",
            open=150.0,
            close=155.0,
            high=156.5,
            low=149.5,
            volume=100000000,
            amount=1.55e10,
            amplitude=4.67,
            change_pct=3.33,
            change_amt=5.0,
            turnover_rate=2.5,
            update_time=datetime.now()
        )
        session.add(test_record)
        await session.commit()

async def main():
    await create_tables()
    await insert_test_data()
    print("操作成功")

if __name__ == "__main__":
    asyncio.run(main())