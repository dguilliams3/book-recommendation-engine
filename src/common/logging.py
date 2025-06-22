import os, sys, json, logging, time, uuid
from aiokafka import AIOKafkaProducer
from common import SettingsInstance as S
from typing import Optional

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_TO_KAFKA = os.getenv("LOG_TO_KAFKA", "1") not in ("0", "false", "False")
SERVICE_NAME = os.getenv("SERVICE_NAME") or os.path.basename(sys.argv[0])
KAFKA_TOPIC = "service_logs"

class KafkaLogHandler(logging.Handler):
    def __init__(self, producer: Optional[AIOKafkaProducer]=None):
        super().__init__()
        self.producer = producer
    def emit(self, record):
        try:
            msg = self.format(record)
            if self.producer and self.producer._closed.done() is False:
                # Async send, fire-and-forget
                self.producer.send(KAFKA_TOPIC, msg.encode())
        except Exception:
            pass

def get_logger(name=None, kafka_producer: Optional[AIOKafkaProducer]=None):
    logger = logging.getLogger(name or SERVICE_NAME)
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    # Formatter for JSON logs
    class JsonFormatter(logging.Formatter):
        def format(self, record):
            base = {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
                "service": SERVICE_NAME,
                "level": record.levelname,
                "event": record.getMessage(),
                "logger": record.name,
                "request_id": getattr(record, "request_id", None),
            }
            if record.exc_info:
                base["exc_info"] = self.formatException(record.exc_info)
            return json.dumps(base)
    formatter = JsonFormatter()
    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(ch)
    # Kafka handler
    if LOG_TO_KAFKA and kafka_producer:
        if not any(isinstance(h, KafkaLogHandler) for h in logger.handlers):
            kh = KafkaLogHandler(kafka_producer)
            kh.setFormatter(formatter)
            logger.addHandler(kh)
    logger.propagate = False
    return logger 