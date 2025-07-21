import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class BaseEvaluator(ABC):
    def __init__(self, predictions, dataloader):
        self.predictions = predictions
        self.dataloader = dataloader

    @abstractmethod
    def evaluate(self):
        pass 