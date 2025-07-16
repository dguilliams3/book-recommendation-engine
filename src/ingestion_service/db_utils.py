from pathlib import Path
from sqlalchemy import text
from common.structured_logging import get_logger
from common.settings import settings as S
import subprocess
import os

logger = get_logger(__name__)


async def _bootstrap_schema(engine):
    sql_dir = S.sql_dir
    sql_files = sorted(sql_dir.glob("*.sql"))
    logger.info(
        "Bootstrapping database schema",
        extra={"sql_files": [str(f) for f in sql_files]},
    )
    try:
        for sql_path in sql_files:
            cmd = [
                "psql",
                f"-h{S.db_host}",
                f"-U{S.db_user}",
                f"-d{S.db_name}",
                "-f", str(sql_path)
            ]
            logger.info(f"Running: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, "PGPASSWORD": S.db_password})
            if result.returncode != 0:
                logger.error(f"psql failed: {result.stderr}")
                raise RuntimeError(f"psql failed: {result.stderr}")
            else:
                logger.info(f"psql output: {result.stdout}")
        logger.info("Database schema bootstrapped successfully")
    except Exception as e:
        logger.error("Failed to bootstrap database schema", exc_info=True)
        raise
