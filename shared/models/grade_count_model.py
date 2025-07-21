from common.src.model.base_model import BaseModel
import pandas as pd

class GradeCountModel(BaseModel):
    def _train(self):
        pass
    def _predict(self):
        df = self.dataloader.data
        if 'grade' in df.columns:
            counts = df['grade'].value_counts()
            return pd.DataFrame([{g: counts.get(g, 0) for g in ['A', 'B', 'C']}])
        else:
            return pd.DataFrame([{'A': 0, 'B': 0, 'C': 0}]) 