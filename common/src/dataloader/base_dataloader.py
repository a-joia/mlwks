import logging
from abc import ABC, abstractmethod
from typing import List, Optional, Iterator, Any

logger = logging.getLogger(__name__)

class BaseDataloader(ABC):
    def __init__(self, preprocessors: Optional[List[Any]] = None):
        self.preprocessors = preprocessors or []
        self.data = None

    @abstractmethod
    def load_data(self, split: str) -> None:
        pass

    def __iter__(self) -> Iterator:
        if self.data is None:
            raise ValueError("Data not loaded. Call load_data(split) first.")
        return iter(self.data)

    def __next__(self):
        if self.data is None:
            raise ValueError("Data not loaded. Call load_data(split) first.")
        return next(self.data)

    def batch_iter(self, batch_size: int) -> Iterator:
        if self.data is None:
            raise ValueError("Data not loaded. Call load_data(split) first.")
        for i in range(0, len(self.data), batch_size):
            yield self.data[i:i+batch_size] 

    def setup(self, **kwargs):
        """Optional setup method for dataloader. E.g., to set batch size, shuffle, etc before each stage"""
        return
    
