from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple


class Flattener(ABC):
    """Convert a structured row dict into (text, metadata) tuple for embeddings."""

    @abstractmethod
    def __call__(self, row: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        raise NotImplementedError
