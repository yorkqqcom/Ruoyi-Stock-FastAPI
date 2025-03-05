"""
股票数据ETL管道 - 增强版（支持自动建表及字段转换）
功能：每日批量获取股票行情及概念数据，支持自动创建数据表
更新说明：添加表自动创建功能，优化数据类型映射
"""
import numpy as np
from module_task.DatabaseManager import DatabaseManager
import logging
import asyncio
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List
import akshare as ak
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_log,
)

# 配置常量
DB_CONFIG = "mysql+pymysql://ruoyi:ruoyi@127.0.0.1:3306/ruoyi-fastapi?charset=utf8mb4"
MAX_WORKERS = 1
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# 字段映射配置（根据AKShare文档定期更新）
FIELD_MAPPING = {
    "stock_zh_a_spot_em": {
        '代码': 'symbol',
        '名称': 'name',
        '最新价': 'price',
        '涨跌幅': 'change_pct',
        '涨跌额': 'change_amt',
        '成交量': 'volume',
        '成交额': 'turnover',
        '振幅': 'amplitude',
        '最高': 'high',
        '最低': 'low',
        '今开': 'open',
        '昨收': 'pre_close',
        '量比': 'volume_ratio',
        '换手率': 'turnover_rate',
        '市盈率-动态': 'pe_ratio',
        '市净率': 'pb_ratio',
        '总市值': 'total_market_cap',
        '流通市值': 'circ_market_cap'
    },
    "stock_sector_fund_flow_rank": {
        '名称': 'concept_name',
        '今日排名': 'rank_today',
        '今日涨跌幅': 'change_percent',
        '今日主力净流入-净额': 'main_net_inflow',
        '今日主力净流入-净占比': 'main_net_ratio',
        '今日超大单净流入-净额': 'super_order_net_inflow',
        '今日超大单净流入-净占比': 'super_order_ratio',
        '今日大单净流入-净额': 'big_order_net_inflow',
        '今日大单净流入-净占比': 'big_order_ratio',
        '今日中单净流入-净额': 'medium_order_net_inflow',
        '今日中单净流入-净占比': 'medium_order_ratio',
        '今日小单净流入-净额': 'small_order_net_inflow',
        '今日小单净流入-净占比': 'small_order_ratio'
    },
    "stock_board_concept_cons_em": {
        '代码': 'symbol',
        '名称': 'name',
        '最新价': 'price',
        '涨跌幅': 'change_pct'
    }
}


class StockDataPipeline:
    """数据管道主类（自动建表版）"""

    def __init__(self):
        self.db = DatabaseManager(DB_CONFIG)
        self.max_workers = MAX_WORKERS
        self.today = datetime.now().date()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, max=60),
        retry=retry_if_exception_type((
            ConnectionError,
            requests.exceptions.RequestException,
            OperationalError,
            SQLAlchemyError
        )),
        before=before_log(logger, logging.INFO),
    )
    def execute_daily_task(self):
        """每日任务入口"""
        # if datetime.now().hour < 15:
        #     raise RuntimeError("请于15:00后获取完整数据")

        logger.info("启动每日数据更新流程")
        try:
            # 股票行情数据
            stock_df = self._fetch_stock_data()
            self.db.bulk_upsert("stock_basic", stock_df, ["symbol"])

            # 新增：将今日数据写入stock_hist表
            self._save_daily_hist(stock_df)

            # 概念资金流数据
            concept_df = self._fetch_concept_flow()
            self.db.bulk_upsert("stock_concept_flow", concept_df, ["concept_name"])


            # 新增：更新概念历史资金流表（当日数据）
            self._save_daily_concept_hist(concept_df)

            # 并行处理概念成分股
            self._process_concept_components()


        except Exception as e:
            logger.error(f"任务执行异常: {str(e)}")
            raise
        finally:
            logger.info("每日数据更新流程结束")

    def _save_daily_hist(self, df: pd.DataFrame):
        """保存历史数据"""
        if df.empty:
            return

        # 字段转换
        hist_df = (
            df.rename(columns={'price': 'close', 'turnover': 'amount'})
            .assign(
                date=self.today,
                adjust_type='none',
                update_time=datetime.now()
            )
            [['date', 'symbol', 'open', 'close', 'high', 'low',
              'volume', 'amount', 'amplitude', 'change_pct',
              'change_amt', 'turnover_rate', 'adjust_type', 'update_time']]
        )

        # 使用事务处理
        try:
            # 分批次处理（每500条提交一次）
            batch_size = 500
            total_rows = len(hist_df)

            with self.db.engine.begin() as conn:
                # 删除操作单独事务
                del_sql = text("""
                        DELETE FROM stock_hist 
                        WHERE date = :date AND adjust_type = 'none'
                    """)
                conn.execute(del_sql, {'date': self.today})

                # 分批次插入
                for i in range(0, total_rows, batch_size):
                    batch_df = hist_df.iloc[i:i + batch_size]
                    temp_table = f"temp_stock_hist_{datetime.now().strftime('%Y%m%d%H%M%S')}"

                    # 创建临时表
                    conn.execute(text(f"""
                            CREATE TEMPORARY TABLE {temp_table} LIKE stock_hist
                        """))

                    # 批量写入临时表
                    batch_df.to_sql(
                        name=temp_table,
                        con=conn,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )

                    # 执行合并操作
                    merge_sql = text(f"""
                            INSERT INTO stock_hist
                            SELECT * FROM {temp_table}
                            ON DUPLICATE KEY UPDATE
                                open=VALUES(open),
                                close=VALUES(close),
                                high=VALUES(high),
                                low=VALUES(low),
                                volume=VALUES(volume),
                                amount=VALUES(amount),
                                amplitude=VALUES(amplitude),
                                change_pct=VALUES(change_pct),
                                change_amt=VALUES(change_amt),
                                turnover_rate=VALUES(turnover_rate),
                                update_time=VALUES(update_time)
                        """)
                    conn.execute(merge_sql)

                    # 删除临时表
                    conn.execute(text(f"DROP TEMPORARY TABLE {temp_table}"))
                logger.info(f"数据合并完成: stock_hist 影响行数: {total_rows}")
        except SQLAlchemyError as e:
            logger.error(f"数据库操作失败: {str(e)}")
            raise

    def _fetch_stock_data(self) -> pd.DataFrame:
        """获取股票行情数据"""
        logger.info("获取实时行情数据")
        try:
            df = ak.stock_zh_a_spot_em()
            return self._transform_data(df, "stock_zh_a_spot_em", ["symbol"])
        except Exception as e:
            logger.error(f"行情数据获取失败: {str(e)}")
            raise

    def _fetch_concept_flow(self) -> pd.DataFrame:
        """获取概念资金流数据"""
        logger.info("获取概念资金流数据")
        try:
            df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="概念资金流")
            return self._transform_data(df, "stock_sector_fund_flow_rank", ["concept_name"])
        except Exception as e:
            logger.error(f"资金流数据获取失败: {str(e)}")
            raise

    def _process_concept_components(self):
        """并行处理概念成分股"""
        logger.info("启动概念成分股处理")
        try:
            with self.db.transaction() as conn:
                concepts = self._get_active_concepts(conn)

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self._process_single_concept, concept)
                          for concept in concepts]

                for future in futures:
                    try:
                        future.result(timeout=300)
                    except Exception as e:
                        logger.error(f"概念处理异常: {str(e)}")
        except Exception as e:
            logger.error(f"成分股处理失败: {str(e)}")
            raise

    def _get_active_concepts(self, conn) -> List[str]:
        """获取有效概念列表"""
        try:
            result = conn.execute(text("SELECT DISTINCT concept_name FROM stock_concept_flow"))
            return [row[0] for row in result.fetchall()]
        except Exception as e:
            logger.error(f"概念列表获取失败: {str(e)}")
            return []

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def _process_single_concept(self, concept_name: str):
        """处理单个概念成分股"""
        logger.info(f"处理概念板块: {concept_name}")
        try:
            df = ak.stock_board_concept_cons_em(symbol=concept_name)
            df = self._transform_data(df, "stock_board_concept_cons_em", ["symbol"])

            # 添加元数据
            df["concept"] = concept_name
            df["update_time"] = datetime.now()

            self.db.bulk_upsert("stock_concept_cons", df, ["symbol", "concept"])
        except Exception as e:
            logger.error(f"[{concept_name}] 处理失败: {str(e)}")
            raise




    def _transform_data(self, df: pd.DataFrame, api_name: str, required_cols: list) -> pd.DataFrame:
        """通用数据转换方法"""
        if df.empty:
            raise ValueError(f"空数据异常: {api_name}")

        mapping = FIELD_MAPPING.get(api_name, {})
        df = df.rename(columns=mapping)

        # 验证必要字段
        missing = set(required_cols) - set(df.columns)
        if missing:
            raise KeyError(f"缺失必要字段: {missing}")

        # 保留映射字段
        keep_cols = [col for col in mapping.values() if col in df.columns]
        return df[keep_cols].dropna(subset=required_cols)

    def _save_daily_concept_hist(self, concept_df: pd.DataFrame):
        """保存当日概念资金流历史数据"""
        if concept_df.empty:
            logger.warning("空概念数据跳过处理")
            return

        # 字段转换映射（新增映射配置）
        CONCEPT_HIST_MAPPING = {
            'main_net_inflow': 'main_net_inflow',
            'main_net_ratio': 'main_net_ratio',
            'super_order_net_inflow': 'super_order_inflow',
            'super_order_ratio': 'super_order_ratio',
            'big_order_net_inflow': 'big_order_inflow',
            'big_order_ratio': 'big_order_ratio',
            'medium_order_net_inflow': 'medium_order_inflow',
            'medium_order_ratio': 'medium_order_ratio',
            'small_order_net_inflow': 'small_order_inflow',
            'small_order_ratio': 'small_order_ratio'
        }

        try:
            # 转换字段并添加必要列
            hist_df = (
                concept_df.rename(columns=CONCEPT_HIST_MAPPING)
                .assign(
                    date=self.today,
                    update_time=datetime.now()
                )
                .pipe(self._process_concept_ratios)  # 处理百分比
                [['date', 'concept_name', 'main_net_inflow', 'main_net_ratio',
                  'super_order_inflow', 'super_order_ratio', 'big_order_inflow',
                  'big_order_ratio', 'medium_order_inflow', 'medium_order_ratio',
                  'small_order_inflow', 'small_order_ratio', 'update_time']]
            )

            # 使用数据库管理器的批量更新方法
            self.db.bulk_upsert(
                table_name="concept_hist_flow",
                df=hist_df,
                unique_keys=["concept_name", "date"]
            )
            logger.info(f"概念历史数据更新完成: 共处理{len(hist_df)}条记录")

        except Exception as e:
            logger.error(f"概念历史数据保存失败: {str(e)}")
            raise

    def _process_single_concept_hist_flow(self, concept_name: str):
        """处理单个概念的当日资金流数据"""
        logger.info(f"处理概念资金流: {concept_name}")
        try:
            # 获取历史资金流数据
            df = ak.stock_concept_fund_flow_hist(symbol=concept_name)
            if df.empty:
                logger.warning(f"空数据: {concept_name}")
                return

            # 数据转换
            df = self._transform_data(df, "stock_concept_fund_flow_hist", ["date"])
            df['concept_name'] = concept_name  # 确保概念名称字段存在

            # 筛选当日数据
            today_str = self.today.strftime("%Y-%m-%d")
            df_today = df[df['date'] == today_str]

            if not df_today.empty:
                # 数据持久化
                self.db.bulk_upsert(
                    table_name="concept_hist_flow",
                    df=df_today,
                    unique_keys=["concept_name", "date"]
                )
                logger.info(f"{concept_name} 当日数据更新成功")
            else:
                logger.warning(f"{concept_name} 无当日数据")

        except Exception as e:
            logger.error(f"{concept_name} 处理异常: {str(e)}")
            raise

    def _process_concept_ratios(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理百分比字段转换"""
        ratio_cols = [col for col in df.columns if 'ratio' in col]
        df[ratio_cols] = df[ratio_cols].replace('%', '', regex=True).astype(float) / 100
        return df

def job():
    """异步任务入口"""
    logger.info("启动异步更新任务")
    try:
        StockDataPipeline().execute_daily_task()
    except Exception as e:
        logger.error(f"任务异常: {str(e)}")
        raise



# 初始化数据库索引（只需执行一次）
def init_database_index(engine):
    index_sql = """
        -- 原有索引
        CREATE INDEX IF NOT EXISTS idx_date_adjust ON stock_hist (date, adjust_type);

        -- 新增唯一索引
        ALTER TABLE stock_hist 
        ADD UNIQUE INDEX uq_date_symbol_adjust (date, symbol, adjust_type);

        -- 查询优化索引
        CREATE INDEX IF NOT EXISTS idx_symbol_update ON stock_hist (symbol, update_time);
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(index_sql))
    except Exception as e:
        print(f"索引创建失败: {str(e)}")
if __name__ == "__main__":
    try:
        job()
    except Exception as e:
        logger.error(f"主程序错误: {str(e)}")
        exit(1)