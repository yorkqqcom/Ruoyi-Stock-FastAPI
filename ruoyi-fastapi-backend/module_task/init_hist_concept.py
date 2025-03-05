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
MAX_WORKERS = 1
# 字段映射配置
FIELD_MAPPING = {
    "stock_concept_fund_flow_hist": {
        '日期': 'date',
        '主力净流入-净额': 'main_net_inflow',
        '主力净流入-净占比': 'main_net_ratio',
        '超大单净流入-净额': 'super_order_inflow',
        '超大单净流入-净占比': 'super_order_ratio',
        '大单净流入-净额': 'big_order_inflow',
        '大单净流入-净占比': 'big_order_ratio',
        '中单净流入-净额': 'medium_order_inflow',
        '中单净流入-净占比': 'medium_order_ratio',
        '小单净流入-净额': 'small_order_inflow',
        '小单净流入-净占比': 'small_order_ratio'
    }
}


class ConceptHistFlowPipeline:
    """概念历史资金流数据管道"""

    def __init__(self):
        self.db = DatabaseManager(DB_CONFIG)
        self.max_workers = MAX_WORKERS  # 并发线程数

    def execute_hist_task(self,
                          start_date: str = '20100101',
                          end_date: str = None):
        """执行历史数据采集任务"""
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")

        # 清空历史表记录（修改后的部分）
        logger.info("清空历史表记录")
        with self.db.engine.connect() as connection:  # 使用独立连接
            with connection.begin():  # 开启显式事务
                connection.execute(text("TRUNCATE TABLE concept_hist_flow"))

        concepts = self._get_all_concepts()
        logger.info(f"获取到{len(concepts)}个待处理概念")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for concept in concepts:
                executor.submit(
                    self._process_single_concept,
                    concept,
                    start_date,
                    end_date
                )

    def _get_all_concepts(self) -> list:
        """从stock_concept_flow表获取所有概念名称"""
        with self.db.transaction() as conn:
            result = conn.execute(text("SELECT DISTINCT concept_name FROM stock_concept_flow"))
            return [row[0] for row in result.fetchall()]

    def _process_single_concept(self,
                                concept: str,
                                start_date: str,
                                end_date: str):
        """处理单个概念历史数据"""
        logger.info(f"正在处理概念: {concept}")
        try:
            # 获取原始数据
            df = ak.stock_concept_fund_flow_hist(symbol=concept)

            if df.empty:
                logger.warning(f"空数据: {concept}")
                return

            # 数据转换
            df = self._transform_data(df, concept)

            # 日期过滤
            df = df[(df['date'] >= pd.to_datetime(start_date)) &
                    (df['date'] <= pd.to_datetime(end_date))]

            # 数据持久化
            self.db.bulk_upsert(
                table_name="concept_hist_flow",
                df=df,
                unique_keys=["concept_name", "date"]
            )

        except Exception as e:
            logger.error(f"{concept} 处理失败: {str(e)}")
            raise

    def _transform_data(self, df: pd.DataFrame, concept_name: str) -> pd.DataFrame:
        """数据转换处理"""
        # 字段映射
        df = df.rename(columns=FIELD_MAPPING["stock_concept_fund_flow_hist"])

        # 添加元数据
        df['concept_name'] = concept_name
        df['update_time'] = datetime.now()

        # 类型转换
        df['date'] = pd.to_datetime(df['date'])

        # 处理百分比值
        ratio_cols = [col for col in df.columns if 'ratio' in col]
        df[ratio_cols] = df[ratio_cols] / 100  # 转换百分比为小数

        # 处理空值
        numeric_cols = df.select_dtypes(include=np.number).columns
        df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], np.nan)

        return df.dropna(subset=['date', 'concept_name'])


def job():
    """概念历史数据采集任务入口"""
    try:
        logger.info("启动概念历史资金流采集任务")

        pipeline = ConceptHistFlowPipeline()
        pipeline.execute_hist_task(
            start_date='20100101'
        )

    except Exception as e:
        logger.error(f"任务执行失败: {str(e)}")
        raise


if __name__ == "__main__":
    job()