import akshare as ak
import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from typing import Optional
from module_task.DatabaseManager import DatabaseManager

DB_CONFIG = "mysql+pymysql://ruoyi:ruoyi@127.0.0.1:3306/ruoyi-fastapi?charset=utf8mb4"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class StockProcessor:
    """股票数据处理核心类"""

    def __init__(self):
        self.db = DatabaseManager(DB_CONFIG)
        self.max_workers = 1  # 根据网络情况调整并发数

    def calculate_history_stats(self, start_date: str = '20100101', end_date: Optional[str] = None):
        """
        计算历史数据统计指标（增加按年分块处理）
        :param start_date: 开始日期(YYYYMMDD)
        :param end_date: 结束日期(YYYYMMDD)，默认当天
        """
        end_date = end_date or datetime.now().strftime('%Y%m%d')

        # 生成年份区间列表
        year_ranges = self._generate_year_ranges(start_date, end_date)

        logger.info(f"开始分年处理统计指标[{start_date}-{end_date}] 共{len(year_ranges)}个年度")

        # 按年度循环处理
        for year_start, year_end in year_ranges:
            logger.info(f"▶ 开始处理 {year_start[:4]} 年度数据")
            try:
                self._process_single_year(year_start, year_end)
            except Exception as e:
                logger.error(f"{year_start[:4]}年度处理失败: {str(e)}")
                raise

    def _generate_year_ranges(self, start_date: str, end_date: str) -> list:
        """生成年度区间列表（优化边界条件）"""
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])

        ranges = []
        for year in range(start_year, end_year + 1):
            # 生成年度默认首尾日期
            year_start = f"{year}0101"
            year_end = f"{year}1231"

            # 当处理起始年时
            if year == start_year:
                # 显式比较日期字符串
                if start_date > year_start:
                    year_start = start_date

            # 当处理结束年时
            if year == end_year:
                # 显式比较日期字符串
                if end_date < year_end:
                    year_end = end_date

            ranges.append((year_start, year_end))
        return ranges

    def _process_single_year(self, year_start: str, year_end: str):
        """处理单个年度数据"""
        process_start = datetime.now()
        # 计算需要缓冲的起始日期
        buffer_start = self._get_trading_day_buffer(year_start, days=15)
        logger.debug(f"数据加载范围: {buffer_start} 至 {year_end}")

        # 阶段2：加载数据（包含缓冲期）
        df = pd.read_sql(f"""
            SELECT symbol, date, adjust_type, close 
            FROM stock_hist 
            WHERE date BETWEEN '{buffer_start}' AND '{year_end}'
            ORDER BY symbol, adjust_type, date
        """, self.db.engine)

        if df.empty:
            logger.warning(f"{year_start[:4]}年度无有效数据，跳过处理")
            return
        # 空值处理（新增前值填充）
        if df['close'].isnull().any():
            df['close'] = df.groupby(['symbol', 'adjust_type'])['close'].ffill()
            df = df.dropna(subset=['close'])

        # 阶段3：指标计算
        stats_df = self._calculate_multi_period_stats(df)
        # 指标计算后校验
        if stats_df.empty:
            logger.warning(f"{year_start[:4]}年度未生成有效指标，跳过保存")
            return

        # 阶段4：保存本年数据
        if not stats_df.empty:
            self.db.bulk_upsert(
                table_name='stock_hist_stats',
                df=stats_df,
                unique_keys=['symbol', 'date', 'adjust_type']
            )

        logger.info(f"▷ {year_start[:4]}年度处理完成，耗时: {(datetime.now() - process_start).total_seconds():.1f}s")


    def _calculate_multi_period_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算多周期统计指标（修复无限值问题）"""
        import numpy as np

        periods = [2, 3, 5, 10]
        if df.empty:
            return pd.DataFrame()

        grouped = df.groupby(['symbol', 'adjust_type'])
        stats_dfs = []

        for (symbol, adjust_type), group in grouped:
            group = group.sort_values('date').reset_index(drop=True)


            if len(group) < max(periods):
                continue

            for period in periods:
                group[f'change_amt_{period}d'] = group['close'].diff(period)
                group[f'change_pct_{period}d'] = group['close'].pct_change(period) * 100

            keep_cols = ['symbol', 'date', 'adjust_type']
            keep_cols += [f'change_{t}_{period}d' for period in periods for t in ['amt', 'pct']]

            stats_df = group[keep_cols].dropna(
                subset=[f'change_pct_{period}d' for period in periods],
                how='all'
            )

            # 新增处理无限值
            stats_df = stats_df.replace([np.inf, -np.inf], np.nan)
            stats_df = stats_df.dropna(how='any')

            if not stats_df.empty:
                stats_dfs.append(stats_df)  # 修复：删除重复添加

            # 增加数据排序验证


        return pd.concat(stats_dfs, ignore_index=True) if stats_dfs else pd.DataFrame()

    def _get_trading_day_buffer(self, date_str: str, days: int = 10) -> str:
        """获取指定日期前N个交易日的日期（精确算法）"""
        # 查询最近的N+1个交易日
        trading_days = pd.read_sql(f"""
            SELECT date 
            FROM (
                SELECT DISTINCT date 
                FROM stock_hist 
                WHERE date <= '{date_str}'
                ORDER BY date DESC 
                LIMIT {days + 10}  -- 扩大查询范围应对节假日
            ) AS t 
            ORDER BY date ASC
        """, self.db.engine)['date'].tolist()

        # 找到第N个交易日前的位置
        if len(trading_days) > days:
            target_date = trading_days[-days-1]
            return target_date.strftime('%Y%m%d')
        return date_str  # 没有足够数据时返回原日期

if __name__ == "__main__":
    # 配置数据库连接（示例使用MySQL）


    processor = StockProcessor()

    # 执行统计计算（示例计算2023年数据）
    try:
        processor.calculate_history_stats(
            start_date='20100101',

        )
        logger.info("统计指标计算任务完成")
    except Exception as e:
        logger.error(f"任务执行失败: {str(e)}")