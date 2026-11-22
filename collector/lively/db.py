from psycopg_pool import AsyncConnectionPool

from .config import CONCURRENCY, DATABASE_URL

pool = AsyncConnectionPool(
    DATABASE_URL,
    min_size=1,
    max_size=CONCURRENCY + 4,
    open=False,
)
