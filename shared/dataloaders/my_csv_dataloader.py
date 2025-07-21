import pandas as pd
from common.src.dataloader.base_dataloader import BaseDataloader
from pydantic import BaseModel
import os

class MyCsvDataloaderInput(BaseModel):
    path: str

class MyCsvDataloader(BaseDataloader):
    def __init__(self, preprocessors=None, filepath:str=""):
        super().__init__(preprocessors=preprocessors)
        print(f"MyCsvDataloader initialized with filepath: {filepath}")
        self.path = filepath

    def load_data(self, split: str):
        # For test, ignore split and just load the file
        self.data = pd.read_csv('example/sample.csv')

    def get_data(self,**kwargs):
        path = self.path
        if self.path.startswith('/app/'):
            path = self.path[5:]  # Remove '/app/'
        return pd.read_csv(path)

    def setup(self, split, batch_size=32, shuffle=False):
        self.split = split
        self.batch_size = batch_size
        self.shuffle = shuffle