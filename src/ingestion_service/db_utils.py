from pathlib import Path
from sqlalchemy import text
from common.structured_logging import get_logger
from common.settings import settings as S

logger = get_logger(__name__)


async def _bootstrap_schema(engine):
    sql_dir = S.sql_dir
    sql_files = sorted(sql_dir.glob("*.sql"))
    logger.info(
        "Bootstrapping database schema",
        extra={"sql_files": [str(f) for f in sql_files]},
    )
    try:
        async with engine.begin() as conn:
            for sql_path in sql_files:
                sql = sql_path.read_text()
                statements = [s.strip() for s in sql.split(";") if s.strip()]
                for stmt in statements:
                    await conn.execute(text(stmt))
            logger.info("Database schema bootstrapped successfully")
    except Exception as e:
        logger.error("Failed to bootstrap database schema", exc_info=True)
        raise
