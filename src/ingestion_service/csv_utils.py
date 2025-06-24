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
                    # Fail fast if DictReader produced extra columns (key == None) which indicates more cells than headers.
                    if k is None or (isinstance(k, str) and k.strip() == ""):
                        extra_cells = v if isinstance(v, list) else [v]
                        logger.error(
                            "Malformed CSV row: extra cells detected", 
                            extra={
                                "file_path": str(path), 
                                "line_num": reader.line_num, 
                                "extra_cells": extra_cells,
                            },
                        )
                        raise ValueError(
                            f"{path.name}: line {reader.line_num} contains {len(extra_cells)} extra value(s) – "
                            "likely due to an unquoted comma or trailing delimiter."
                        )

                    # If the CSV parser produced a list (e.g., unescaped commas), join into one string
                    if isinstance(v, list):
                        v = ",".join(str(x) for x in v)

                    # Normalise whitespace and empty strings
                    if v is None or str(v).strip() == "":
                        cleaned_row[k] = None
                    else:
                        cleaned_row[k] = str(v).strip()
                yield cleaned_row
            logger.info("CSV file loaded successfully", extra={"file_path": str(path), "row_count": row_count})
    except Exception as e:
        logger.error(f"Failed to load CSV file: {e}", exc_info=True, extra={"file_path": str(path)})
        raise 