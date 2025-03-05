
import uuid
import logging
from datetime import datetime
from typing import List, Dict

import pandas as pd
from sqlalchemy import create_engine, MetaData, text, inspect

from contextlib import contextmanager
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
class DatabaseManager:
    """数据库连接管理器（支持自动建表）"""

    def __init__(self, conn_str: str):
        self.engine = create_engine(
            conn_str,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            execution_options={
                "isolation_level": "READ COMMITTED",  # 修改隔离级别[4](@ref)
                "innodb_lock_wait_timeout": 120  # 增加锁等待时间[2,9](@ref)
            }
        )
        self.metadata = MetaData()
        self.inspector = inspect(self.engine)

    def _pandas_dtype_to_sql(self, dtype, col_name=None):
        """Pandas 类型转 SQL 类型（精确处理关键字段）"""
        if col_name in ['symbol']:
            return 'VARCHAR(20)'
        elif pd.api.types.is_integer_dtype(dtype):
            return 'BIGINT'
        elif pd.api.types.is_float_dtype(dtype):
            return 'DOUBLE'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'DATETIME'
        elif pd.api.types.is_string_dtype(dtype):
            return 'VARCHAR(255)'
        else:
            return 'VARCHAR(255)'

    @contextmanager
    def transaction(self):
        """事务上下文管理器"""
        conn = self.engine.connect()
        trans = conn.begin()
        try:
            yield conn
            trans.commit()
        except Exception as e:
            trans.rollback()
            logger.error(f"事务回滚: {str(e)}")
            raise
        finally:
            conn.close()

    def _create_table(self, table_name: str, df: pd.DataFrame, unique_keys: List[str]):
        """动态创建数据表"""
        columns = []
        for col_name, dtype in df.dtypes.items():
            sql_type = self._pandas_dtype_to_sql(dtype, col_name)
            columns.append(f"`{col_name}` {sql_type}")

        # 添加主键约束
        if unique_keys:
            pk_columns = ', '.join([f"`{key}`" for key in unique_keys])
            columns.append(f"PRIMARY KEY ({pk_columns})")

        create_sql = f"""CREATE TABLE `{table_name}` (
            {',\n'.join(columns)}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"""

        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_sql))
                conn.commit()
                logger.info(f"表创建成功: {table_name}")
        except Exception as e:
            logger.error(f"表创建失败: {str(e)}\nSQL: {create_sql}")
            raise

    def _validate_table_schema(self, table_name: str, df: pd.DataFrame, unique_keys: List[str]):
        """表结构验证及自动创建"""
        try:
            if not self.inspector.has_table(table_name):
                logger.info(f"检测到表不存在: {table_name}")
                self._create_table(table_name, df, unique_keys)
            else:
                # 验证字段兼容性
                existing_columns = {col['name'] for col in self.inspector.get_columns(table_name)}
                missing_columns = set(df.columns) - existing_columns
                if missing_columns:
                    raise ValueError(f"表字段缺失: {table_name} 缺少 {', '.join(missing_columns)}")
        except Exception as e:
            logger.error(f"表结构验证失败: {str(e)}")
            raise

    def bulk_upsert(self, table_name: str, df: pd.DataFrame, unique_keys: List[str]):
        """增强版批量更新插入"""
        if df.empty:
            logger.warning(f"空数据跳过写入: {table_name}")
            return

        # 前置校验
        self._validate_table_schema(table_name, df, unique_keys)

        # 生成临时表名
        temp_table = f"temp_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:6]}"

        try:
            with self.transaction() as conn:
                # 创建临时表（精确类型控制）
                df.to_sql(
                    name=temp_table,
                    con=conn,
                    index=False,
                    if_exists='replace',
                    chunksize=1000,
                    # dtype={col: self._pandas_dtype_to_sql(df[col].dtype, col) for col in df.columns}
                )

                # 构建合并语句
                columns = [f'`{col}`' for col in df.columns]
                update_set = ', '.join(
                    [f'`{col}`=VALUES(`{col}`)' for col in df.columns if col not in unique_keys]
                )

                merge_sql = f"""
                    INSERT INTO `{table_name}` ({', '.join(columns)})
                    SELECT {', '.join(columns)} FROM `{temp_table}`
                    ON DUPLICATE KEY UPDATE {update_set}
                """
                # print(merge_sql)
                conn.execute(text(merge_sql))
                logger.info(f"数据合并完成: {table_name} 影响行数: {len(df)}")
        except Exception as e:
            logger.error(f"{temp_table}:数据合并失败: {str(e)}")
            raise
        finally:
            self._cleanup_temp_table(temp_table)

    def _cleanup_temp_table(self, temp_table: str):
        """安全清理临时表"""
        try:
            with self.engine.connect() as conn:
                if conn.dialect.has_table(conn, temp_table):
                    conn.execute(text(f"DROP TABLE IF EXISTS `{temp_table}`"))
                    conn.commit()
                    logger.debug(f"临时表清理完成: {temp_table}")
        except Exception as e:
            logger.warning(f"临时表清理异常: {str(e)}")


    # 在DatabaseManager中添加锁监控方法
    def show_lock_status(self):
        lock_sql = text("""
            SELECT 
                r.trx_id waiting_trx_id,
                r.trx_mysql_thread_id waiting_thread,
                b.trx_id blocking_trx_id,
                b.trx_mysql_thread_id blocking_thread
            FROM information_schema.innodb_lock_waits w
            INNER JOIN information_schema.innodb_trx b
              ON b.trx_id = w.blocking_trx_id
            INNER JOIN information_schema.innodb_trx r
              ON r.trx_id = w.requesting_trx_id
        """)
        return pd.read_sql(lock_sql, self.engine)

