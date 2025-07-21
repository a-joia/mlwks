from common.src.model.base_model import BaseModel
import pandas as pd

class RowCountModel(BaseModel):
    def _train(self):
        pass
    def _predict(self):
        df = self.dataloader.data
        count = len(df)
        return pd.DataFrame({'row_count': [count]}) 