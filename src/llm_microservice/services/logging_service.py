"""
Logging service for the LLM microservice.

This module provides structured logging with Kafka producer and file fallback
for requests, metrics, and errors.
"""

import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import os
from threading import Lock
from pathlib import Path

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

from ..models.config import KafkaConfig
from ..utils.errors import LoggingError, sanitize_error_for_logging
from ..utils.retry import retry_with_exponential_backoff, KAFKA_RETRY_CONFIG

logger = logging.getLogger(__name__)


class LoggingService:
    """Structured logging service with Kafka producer and file fallback."""
    
    def __init__(self, config: KafkaConfig, log_file: Optional[str] = None):
        self.config = config
        self.log_file = log_file
        self.kafka_producer = None
        self.enabled = config.enabled and KAFKA_AVAILABLE
        self.file_lock = Lock()
        self.stats = {
            'requests_logged': 0,
            'metrics_logged': 0,
            'errors_logged': 0,
            'kafka_failures': 0,
            'file_fallbacks': 0,
        }
        
        if self.enabled:
            self._initialize_kafka()
        
        # Ensure log file directory exists
        if self.log_file:
            Path(self.log_file).parent.mkdir(parents=True, exist_ok=True)
    
    def _initialize_kafka(self) -> None:
        """Initialize Kafka producer with error handling."""
        try:
            if not KAFKA_AVAILABLE:
                logger.warning("Kafka not available, falling back to file logging")
                self.enabled = False
                return
            
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda v: v.encode('utf-8') if v else None,
                acks='all',  # Wait for all replicas
                retries=self.config.max_retries,
                retry_backoff_ms=1000,
                request_timeout_ms=self.config.producer_timeout * 1000,
                max_in_flight_requests_per_connection=5,
                enable_idempotence=True,
                compression_type='snappy',
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432,
            )
            
            logger.info("Kafka producer initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            self.enabled = False
            self.kafka_producer = None
    
    def _sanitize_log_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize log data by removing sensitive information."""
        return sanitize_error_for_logging(data)
    
    def _write_to_file(self, log_entry: Dict[str, Any]) -> None:
        """Write log entry to file with thread safety."""
        if not self.log_file:
            return
        
        try:
            with self.file_lock:
                with open(self.log_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(log_entry, default=str) + '\n')
                    f.flush()
            
            self.stats['file_fallbacks'] += 1
            
        except Exception as e:
            logger.error(f"Failed to write to log file: {e}")
    
    @retry_with_exponential_backoff(
        max_retries=KAFKA_RETRY_CONFIG.max_retries,
        base_delay=KAFKA_RETRY_CONFIG.base_delay,
        max_delay=KAFKA_RETRY_CONFIG.max_delay,
        retry_exceptions=(KafkaError,),
        stop_exceptions=(),
    )
    def _send_to_kafka(self, topic: str, message: Dict[str, Any], key: Optional[str] = None) -> None:
        """Send message to Kafka with retry logic."""
        if not self.enabled or not self.kafka_producer:
            raise LoggingError("Kafka producer not available")
        
        try:
            future = self.kafka_producer.send(topic, value=message, key=key)
            # Wait for acknowledgment with timeout
            future.get(timeout=self.config.producer_timeout)
            
        except Exception as e:
            logger.error(f"Failed to send message to Kafka topic {topic}: {e}")
            raise
    
    def log_request(
        self,
        request_id: str,
        request_data: Dict[str, Any],
        response_data: Optional[Dict[str, Any]] = None,
        error: Optional[Exception] = None,
        duration_ms: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log LLM request with sanitized data.
        
        Args:
            request_id: The request ID
            request_data: The request data (will be sanitized)
            response_data: The response data (will be sanitized)
            error: Any error that occurred
            duration_ms: Request duration in milliseconds
            metadata: Additional metadata
        """
        try:
            # Sanitize request data
            sanitized_request = self._sanitize_log_data(request_data)
            sanitized_response = self._sanitize_log_data(response_data) if response_data else None
            
            log_entry = {
                'event_type': 'llm_request',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'request_id': request_id,
                'request': sanitized_request,
                'response': sanitized_response,
                'error': str(error) if error else None,
                'duration_ms': duration_ms,
                'metadata': metadata or {},
            }
            
            # Try Kafka first
            if self.enabled:
                try:
                    self._send_to_kafka(
                        topic=self.config.request_topic,
                        message=log_entry,
                        key=request_id
                    )
                    self.stats['requests_logged'] += 1
                    logger.debug(f"Logged request {request_id} to Kafka")
                    return
                except Exception as e:
                    logger.warning(f"Kafka logging failed, falling back to file: {e}")
                    self.stats['kafka_failures'] += 1
            
            # Fall back to file logging
            self._write_to_file(log_entry)
            self.stats['requests_logged'] += 1
            logger.debug(f"Logged request {request_id} to file")
            
        except Exception as e:
            logger.error(f"Failed to log request {request_id}: {e}")
    
    def log_metrics(
        self,
        request_id: str,
        metrics: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log performance metrics.
        
        Args:
            request_id: The request ID
            metrics: The metrics data
            metadata: Additional metadata
        """
        try:
            log_entry = {
                'event_type': 'llm_metrics',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'request_id': request_id,
                'metrics': metrics,
                'metadata': metadata or {},
            }
            
            # Try Kafka first
            if self.enabled:
                try:
                    self._send_to_kafka(
                        topic=self.config.metrics_topic,
                        message=log_entry,
                        key=request_id
                    )
                    self.stats['metrics_logged'] += 1
                    logger.debug(f"Logged metrics for request {request_id} to Kafka")
                    return
                except Exception as e:
                    logger.warning(f"Kafka metrics logging failed, falling back to file: {e}")
                    self.stats['kafka_failures'] += 1
            
            # Fall back to file logging
            self._write_to_file(log_entry)
            self.stats['metrics_logged'] += 1
            logger.debug(f"Logged metrics for request {request_id} to file")
            
        except Exception as e:
            logger.error(f"Failed to log metrics for request {request_id}: {e}")
    
    def log_error(
        self,
        request_id: str,
        error: Exception,
        context: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log error with context.
        
        Args:
            request_id: The request ID
            error: The error that occurred
            context: Error context (will be sanitized)
            metadata: Additional metadata
        """
        try:
            # Sanitize context data
            sanitized_context = self._sanitize_log_data(context) if context else {}
            
            log_entry = {
                'event_type': 'llm_error',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'request_id': request_id,
                'error': {
                    'type': type(error).__name__,
                    'message': str(error),
                    'details': getattr(error, 'details', None),
                },
                'context': sanitized_context,
                'metadata': metadata or {},
            }
            
            # Try Kafka first
            if self.enabled:
                try:
                    self._send_to_kafka(
                        topic=self.config.error_topic,
                        message=log_entry,
                        key=request_id
                    )
                    self.stats['errors_logged'] += 1
                    logger.debug(f"Logged error for request {request_id} to Kafka")
                    return
                except Exception as e:
                    logger.warning(f"Kafka error logging failed, falling back to file: {e}")
                    self.stats['kafka_failures'] += 1
            
            # Fall back to file logging
            self._write_to_file(log_entry)
            self.stats['errors_logged'] += 1
            logger.debug(f"Logged error for request {request_id} to file")
            
        except Exception as e:
            logger.error(f"Failed to log error for request {request_id}: {e}")
    
    def flush(self) -> None:
        """Flush any pending messages."""
        if self.kafka_producer:
            try:
                self.kafka_producer.flush(timeout=10)
                logger.debug("Flushed Kafka producer")
            except Exception as e:
                logger.warning(f"Failed to flush Kafka producer: {e}")
    
    def close(self) -> None:
        """Close the logging service and cleanup resources."""
        if self.kafka_producer:
            try:
                self.kafka_producer.flush(timeout=10)
                self.kafka_producer.close(timeout=10)
                logger.info("Kafka producer closed")
            except Exception as e:
                logger.warning(f"Failed to close Kafka producer: {e}")
            finally:
                self.kafka_producer = None
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get logging statistics.
        
        Returns:
            Dict[str, Any]: Logging statistics
        """
        stats = {
            'enabled': self.enabled,
            'kafka_available': KAFKA_AVAILABLE,
            'kafka_connected': self.kafka_producer is not None,
            'log_file': self.log_file,
            'stats': self.stats.copy(),
            'config': {
                'bootstrap_servers': self.config.bootstrap_servers,
                'request_topic': self.config.request_topic,
                'metrics_topic': self.config.metrics_topic,
                'error_topic': self.config.error_topic,
                'producer_timeout': self.config.producer_timeout,
                'max_retries': self.config.max_retries,
            }
        }
        
        return stats
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on logging service.
        
        Returns:
            Dict[str, Any]: Health check results
        """
        health = {
            'status': 'healthy',
            'kafka_available': KAFKA_AVAILABLE,
            'kafka_connected': False,
            'file_fallback_active': False,
        }
        
        if not self.enabled:
            health['status'] = 'disabled'
            if self.log_file:
                health['file_fallback_active'] = True
                health['status'] = 'degraded'
            return health
        
        # Test Kafka connection
        if self.kafka_producer:
            try:
                # Try to get metadata (this tests the connection)
                metadata = self.kafka_producer.list_topics(timeout=5)
                health['kafka_connected'] = True
                health['status'] = 'healthy'
            except Exception as e:
                logger.warning(f"Kafka health check failed: {e}")
                health['kafka_connected'] = False
                health['file_fallback_active'] = True
                health['status'] = 'degraded'
                health['error'] = str(e)
        else:
            health['file_fallback_active'] = True
            health['status'] = 'degraded'
        
        return health 