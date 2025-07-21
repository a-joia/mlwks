from common.src.model.base_model import BaseModel
import pandas as pd

class SumScoreModel(BaseModel):
    def _train(self):
        pass
    def _predict(self):
        df = self.dataloader.data
        sum_score = df['score'].sum() if 'score' in df.columns else None
        return pd.DataFrame({'sum_score': [sum_score]}) 