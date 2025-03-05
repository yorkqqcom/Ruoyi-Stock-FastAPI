from module_task.DatabaseManager import DatabaseManager
import logging
import akshare as ak
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text
import numpy as np

DB_CONFIG = "mysql+pymysql://ruoyi:ruoyi@127.0.0.1:3306/ruoyi-fastapi?charset=utf8mb4"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 扩展字段映射配置
FIELD_MAPPING = {
    "stock_zh_a_hist": {
        '日期': 'date',
        '股票代码': 'symbol',
        '开盘': 'open',
        '收盘': 'close',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume',
        '成交额': 'amount',
        '振幅': 'amplitude',
        '涨跌幅': 'change_pct',
        '涨跌额': 'change_amt',
        '换手率': 'turnover_rate'
    }
}


class StockHistPipeline:
    """历史行情数据管道"""

    def __init__(self):
        self.db = DatabaseManager(DB_CONFIG)
        self.max_workers = 4  # 根据网络情况调整并发数

    def execute_hist_task(self,
                          period: str = 'daily',
                          adjust: str = '',
                          start_date: str = '19900101',
                          end_date: str = None):
        """
        执行历史数据采集任务
        :param period: 周期 daily/weekly/monthly
        :param adjust: 复权类型 qfq/hfq/空字符串
        :param start_date: 开始日期(YYYYMMDD)
        :param end_date: 结束日期(YYYYMMDD)
        """
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")

        symbols = self._get_all_symbols()
        logger.info(f"获取到{len(symbols)}个待处理股票代码")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for symbol in symbols:
                executor.submit(
                    self._process_single_symbol,
                    symbol,
                    period,
                    adjust,
                    start_date,
                    end_date
                )

    def _get_all_symbols(self) -> list:
        """从stock_basic表获取所有股票代码"""
        with self.db.transaction() as conn:
            result = conn.execute(text("SELECT DISTINCT symbol FROM stock_basic"))
            return [row[0] for row in result.fetchall()]

    def _process_single_symbol(self,
                               symbol: str,
                               period: str,
                               adjust: str,
                               start_date: str,
                               end_date: str):
        """处理单个股票历史数据"""
        logger.info(f"正在处理 {symbol} [{adjust}]")
        try:
            # 获取原始数据
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )

            if df.empty:
                logger.warning(f"空数据: {symbol}")
                return

            # 数据转换
            df = self._transform_data(df, adjust)

            # 数据持久化
            self.db.bulk_upsert(
                table_name="stock_hist",
                df=df,
                unique_keys=["symbol", "date", "adjust_type"]
            )

        except Exception as e:
            logger.error(f"{symbol} 处理失败: {str(e)}")
            raise

    def _transform_data(self, df: pd.DataFrame, adjust_type: str) -> pd.DataFrame:
        """数据转换处理"""
        # 字段映射
        df = df.rename(columns=FIELD_MAPPING["stock_zh_a_hist"])

        # 类型转换
        df['date'] = pd.to_datetime(df['date'])
        df['symbol'] = df['symbol'].astype(str).str.zfill(6)

        # 添加元数据
        df['adjust_type'] = adjust_type if adjust_type else 'none'
        df['update_time'] = datetime.now()

        # 处理无穷值
        numeric_cols = df.select_dtypes(include=np.number).columns
        df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], np.nan)

        return df.dropna(subset=['date', 'symbol'])


def hist_job():
    """历史数据采集任务入口"""
    try:
        logger.info("启动历史数据采集任务")

        pipeline = StockHistPipeline()
        pipeline.execute_hist_task(
            period='daily',
            adjust='hfq',  # 使用后复权数据
            start_date='20100101',
            end_date='20251231'
        )

    except Exception as e:
        logger.error(f"任务执行失败: {str(e)}")
        raise


if __name__ == "__main__":
    hist_job()