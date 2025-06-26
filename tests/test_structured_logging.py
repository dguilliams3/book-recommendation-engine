import json
import logging
from io import StringIO

from common.structured_logging import get_logger


def test_get_logger_json_formatting(capsys):
    logger = get_logger("test_logger")
    logger.info("hello world")
    captured = capsys.readouterr()
    output = captured.out.strip()
    data = json.loads(output)
    assert data["event"] == "hello world"
    assert data["level"] == "INFO"
    assert data["logger"] == "test_logger" 