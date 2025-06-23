import csv
from pathlib import Path
from typing import Iterable
from common.structured_logging import get_logger

logger = get_logger(__name__)

def _load_csv(path: Path) -> Iterable[dict]:
    logger.debug("Loading CSV file", extra={"file_path": str(path)})
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            row_count = 0
            for row in reader:
                row_count += 1
                # Clean empty strings and whitespace
                cleaned_row = {}
                for k, v in row.items():
                    # If the CSV parser produced a list (e.g., unescaped commas), join into one string
                    if isinstance(v, list):
                        v = ",".join(str(x) for x in v)
                    if v is None or str(v).strip() == "":
                        cleaned_row[k] = None
                    else:
                        cleaned_row[k] = str(v).strip()
                yield cleaned_row
            logger.info("CSV file loaded successfully", extra={"file_path": str(path), "row_count": row_count})
    except Exception as e:
        logger.error(f"Failed to load CSV file: {e}", exc_info=True, extra={"file_path": str(path)})
        raise 