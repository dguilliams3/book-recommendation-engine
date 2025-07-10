import os, sys, json, logging, time, uuid
from typing import Optional, Dict, Any
from contextvars import ContextVar

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_TO_KAFKA = os.getenv("LOG_TO_KAFKA", "1") not in ("0", "false", "False")
SERVICE_NAME = os.getenv("SERVICE_NAME") or os.path.basename(sys.argv[0])
KAFKA_TOPIC = "service_logs"

# --- Production Context Variables -------------------------------------------
# These provide request-scoped context that persists across async calls
request_id_var: ContextVar[Optional[str]] = ContextVar('request_id', default=None)
user_id_var: ContextVar[Optional[str]] = ContextVar('user_id', default=None)
session_id_var: ContextVar[Optional[str]] = ContextVar('session_id', default=None)
feature_flags_var: ContextVar[Optional[Dict[str, Any]]] = ContextVar('feature_flags', default=None)

# --- Prometheus setup -------------------------------------------------------
ENABLE_METRICS = os.getenv("ENABLE_METRICS", "1") not in ("0", "false", "False")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9000"))

_metrics_server_started = False

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

def set_request_context(request_id: str = None, user_id: str = None, session_id: str = None, feature_flags: Dict[str, Any] = None):
    """Set request-scoped context variables for logging correlation.
    
    Args:
        request_id: Unique identifier for the request
        user_id: Privacy-safe user identifier (hashed)
        session_id: Session identifier for user journey tracking
        feature_flags: Active feature flags for this request
    """
    if request_id is None:
        request_id = str(uuid.uuid4())
    
    request_id_var.set(request_id)
    if user_id:
        user_id_var.set(user_id)
    if session_id:
        session_id_var.set(session_id)
    if feature_flags:
        feature_flags_var.set(feature_flags)
    
    return request_id

def get_request_context() -> Dict[str, Any]:
    """Get current request context for logging."""
    return {
        "request_id": request_id_var.get(),
        "user_id": user_id_var.get(),
        "session_id": session_id_var.get(),
        "feature_flags": feature_flags_var.get(),
    }

class PerformanceLogger:
    """Context manager for performance logging."""
    
    def __init__(self, logger: logging.Logger, operation: str, **context):
        self.logger = logger
        self.operation = operation
        self.context = context
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        self.logger.info(f"Starting {self.operation}", extra={
            "operation": self.operation,
            "phase": "start",
            **self.context
        })
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        level = logging.ERROR if exc_type else logging.INFO
        status = "error" if exc_type else "success"
        
        self.logger.log(level, f"Completed {self.operation}", extra={
            "operation": self.operation,
            "phase": "complete",
            "duration_seconds": round(duration, 3),
            "status": status,
            "error_type": exc_type.__name__ if exc_type else None,
            **self.context
        })

def get_logger(name: str | None = None, kafka_producer: Optional["AIOKafkaProducer"] = None):
    """Return a JSON-logging logger with production enhancements."""
    try:
        from aiokafka import AIOKafkaProducer  # heavy import delayed
    except ImportError:
        AIOKafkaProducer = None  # type: ignore

    logger = logging.getLogger(name or SERVICE_NAME)
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    class JsonFormatter(logging.Formatter):
        def format(self, record):
            # Get request context
            context = get_request_context()
            
            payload = {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
                "service": SERVICE_NAME,
                "level": record.levelname,
                "event": record.getMessage(),
                "logger": record.name,
                **{k: v for k, v in context.items() if v is not None}
            }
            
            # Add Reader Mode specific context
            if os.getenv("ENABLE_READER_MODE") == "true":
                payload["reader_mode"] = True
            
            if record.exc_info:
                payload["exc_info"] = self.formatException(record.exc_info)
            return json.dumps(payload)

    class KafkaJsonFormatter(logging.Formatter):
        """Enhanced Kafka formatter with full context and performance metrics."""

        _builtin_keys = set(logging.LogRecord(None, 0, "", 0, "", (), None, None).__dict__.keys())

        def format(self, record):
            # Get request context
            context = get_request_context()
            
            # Base payload matches the console formatter for consistency
            payload = {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
                "service": SERVICE_NAME,
                "level": record.levelname,
                "event": record.getMessage(),
                "logger": record.name,
                **{k: v for k, v in context.items() if v is not None}
            }

            # Add environment context
            payload.update({
                "environment": os.getenv("ENVIRONMENT", "development"),
                "version": os.getenv("SERVICE_VERSION", "unknown"),
                "host": os.getenv("HOSTNAME", "unknown"),
                "reader_mode_enabled": os.getenv("ENABLE_READER_MODE", "false") == "true"
            })

            # Merge any user-supplied extras except builtin attributes
            for key, value in record.__dict__.items():
                if key not in self._builtin_keys and key not in payload:
                    payload[key] = value

            if record.exc_info:
                payload["exc_info"] = self.formatException(record.exc_info)

            return json.dumps(payload)

    formatter = JsonFormatter()

    # Console handler
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    # ------------------------------------------------------------------
    # Kafka handler – acquire a producer automatically if not supplied.
    # This is best-effort: if Kafka is down or aiokafka is missing we
    # simply skip the handler and continue with console logging.
    # ------------------------------------------------------------------

    if LOG_TO_KAFKA and AIOKafkaProducer is not None:
        if kafka_producer is None:
            # Try to obtain the shared producer from common.kafka_utils.
            try:
                import asyncio
                from common.kafka_utils import get_event_producer  # lazy import

                async def _obtain():  # noqa: D401
                    try:
                        producer = await get_event_producer()
                        return await producer._get_producer()
                    except Exception:  # pragma: no cover – swallow errors
                        return None

                if asyncio.get_event_loop().is_running():
                    # Can't await in sync context; skip auto-producer.
                    kafka_producer = None
                else:
                    kafka_producer = asyncio.run(_obtain())
            except Exception:
                kafka_producer = None

        if kafka_producer and not any(isinstance(h, KafkaLogHandler) for h in logger.handlers):
            try:
                kh = KafkaLogHandler(kafka_producer)
                kh.setFormatter(KafkaJsonFormatter())
                logger.addHandler(kh)
            except Exception:
                # If handler setup fails we ignore and continue.
                pass

    logger.propagate = False

    # Add performance logging method to logger
    def log_performance(operation: str, **context):
        return PerformanceLogger(logger, operation, **context)
    
    logger.log_performance = log_performance

    # Start Prometheus metrics endpoint once per process (best-effort)
    global _metrics_server_started
    if ENABLE_METRICS and not _metrics_server_started:
        try:
            from prometheus_client import start_http_server
            start_http_server(METRICS_PORT)
            logger.info("Prometheus metrics server started", extra={"port": METRICS_PORT})
            _metrics_server_started = True
        except Exception:
            # Fail silently – metrics are optional.
            logger.debug("Failed to start Prometheus metrics server", exc_info=True)

    return logger

# Convenience functions for common logging patterns
def log_api_request(logger: logging.Logger, method: str, path: str, user_id: str = None, **context):
    """Log API request with standard context."""
    request_id = set_request_context(user_id=user_id)
    logger.info(f"API Request: {method} {path}", extra={
        "api_method": method,
        "api_path": path,
        "request_type": "api",
        **context
    })
    return request_id

def log_business_event(logger: logging.Logger, event_type: str, entity_id: str = None, **context):
    """Log business events for analytics."""
    logger.info(f"Business Event: {event_type}", extra={
        "event_type": event_type,
        "entity_id": entity_id,
        "category": "business",
        **context
    })

def log_error_with_context(logger: logging.Logger, error: Exception, operation: str, **context):
    """Log errors with full context for debugging."""
    logger.error(f"Error in {operation}: {str(error)}", extra={
        "operation": operation,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "category": "error",
        **context
    }, exc_info=True) 