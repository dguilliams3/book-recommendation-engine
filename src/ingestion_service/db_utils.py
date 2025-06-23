from pathlib import Path
from sqlalchemy import text
from common.structured_logging import get_logger

logger = get_logger(__name__)

async def _bootstrap_schema(engine):
    sql_path = Path("sql/00_init_schema.sql")
    logger.info("Bootstrapping database schema", extra={"sql_file": str(sql_path)})
    try:
        async with engine.begin() as conn:
            sql = sql_path.read_text()
            statements = [s.strip() for s in sql.split(';') if s.strip()]
            for stmt in statements:
                await conn.execute(text(stmt))
            logger.info("Database schema bootstrapped successfully")
    except Exception as e:
        logger.error("Failed to bootstrap database schema", exc_info=True)
        raise 