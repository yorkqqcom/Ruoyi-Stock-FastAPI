from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
from contextlib import asynccontextmanager

# 数据库连接配置
DATABASE_URL = "mysql+aiomysql://ruoyi:ruoyi@127.0.0.1:3306/ruoyi-fastapi?charset=utf8mb4"

# 创建异步引擎
engine = create_async_engine(
    DATABASE_URL,
    echo=True,          # 生产环境设置为False
    pool_size=5,       # 连接池大小
    max_overflow=10,   # 最大溢出连接数
    pool_recycle=3600,  # 连接回收时间（秒）
    pool_pre_ping=True  # 执行前测试连接有效性
)

# 创建异步会话工厂
async_session_factory = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)

# 定义依赖注入用的会话获取器
@asynccontextmanager
async def get_async_session() -> AsyncSession:
    """
    异步会话生成器，用于依赖注入
    使用示例：
    async def get_db(session: AsyncSession = Depends(get_async_session)):
        yield session
    """
    session = async_session_factory()
    try:
        yield session
    except Exception as e:
        await session.rollback()
        raise
    finally:
        await session.close()

# 初始化数据库表结构
async def init_db():
    """
    应用启动时初始化数据库表
    在main.py中调用：await init_db()
    """
    async with engine.begin() as conn:
        # 生产环境建议禁用自动建表
        await conn.run_sync(SQLModel.metadata.create_all)