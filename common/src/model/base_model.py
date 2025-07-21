import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class BaseModel(ABC):
    def __init__(self, dataloader, split: str = "train"):
        self.dataloader = dataloader
        self.split = split

    def train(self):
        self.dataloader.load_data(self.split)
        self._train()

    def predict(self, split: str = None):
        split = split or self.split
        self.dataloader.load_data(split)
        return self._predict()

    @abstractmethod
    def _train(self):
        pass

    @abstractmethod
    def _predict(self):
        pass 