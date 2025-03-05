# \\models\stock.py
from datetime import datetime
from sqlmodel import SQLModel, Field


class StockHist(SQLModel, table=True):
    __tablename__ = "stock_hist"

    date: datetime = Field(primary_key=True)
    symbol: str = Field(max_length=20, primary_key=True)
    open: float = Field(nullable=True)
    close: float = Field(nullable=True)
    high: float = Field(nullable=True)
    low: float = Field(nullable=True)
    volume: int = Field(nullable=True)
    amount: float = Field(nullable=True)
    amplitude: float = Field(nullable=True)
    change_pct: float = Field(nullable=True)
    change_amt: float = Field(nullable=True)
    turnover_rate: float = Field(nullable=True)
    adjust_type: str = Field(primary_key=True)
    update_time: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={"onupdate": datetime.utcnow}
    )