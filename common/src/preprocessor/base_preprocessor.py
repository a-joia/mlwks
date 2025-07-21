import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class BasePreprocessor(ABC):
    @abstractmethod
    def process(self, data):
        pass 