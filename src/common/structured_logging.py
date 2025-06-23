import os, sys, json, logging, time, uuid
from typing import Optional

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_TO_KAFKA = os.getenv("LOG_TO_KAFKA", "1") not in ("0", "false", "False")
SERVICE_NAME = os.getenv("SERVICE_NAME") or os.path.basename(sys.argv[0])
KAFKA_TOPIC = "service_logs"

class KafkaLogHandler(logging.Handler):
    def __init__(self, producer: Optional["AIOKafkaProducer"] = None):
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

def get_logger(name: str | None = None, kafka_producer: Optional["AIOKafkaProducer"] = None):
    """Return a JSON-logging logger. Importing aiokafka lazily avoids heavy deps when Kafka not used."""
    try:
        from aiokafka import AIOKafkaProducer  # heavy import delayed
    except ImportError:
        AIOKafkaProducer = None  # type: ignore

    logger = logging.getLogger(name or SERVICE_NAME)
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    class JsonFormatter(logging.Formatter):
        def format(self, record):
            payload = {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
                "service": SERVICE_NAME,
                "level": record.levelname,
                "event": record.getMessage(),
                "logger": record.name,
                "request_id": getattr(record, "request_id", None),
            }
            if record.exc_info:
                payload["exc_info"] = self.formatException(record.exc_info)
            return json.dumps(payload)

    formatter = JsonFormatter()

    # Console handler
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    # Kafka handler
    if LOG_TO_KAFKA and kafka_producer and AIOKafkaProducer is not None:
        if not any(isinstance(h, KafkaLogHandler) for h in logger.handlers):
            kh = KafkaLogHandler(kafka_producer)
            kh.setFormatter(formatter)
            logger.addHandler(kh)

    logger.propagate = False
    return logger 