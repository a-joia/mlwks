from common.src.model.base_model import BaseModel
import pandas as pd

class MeanScoreModel(BaseModel):
    def _train(self):
        pass
    def _predict(self):
        df = self.dataloader.data
        mean_score = df['score'].mean() if 'score' in df.columns else None
        return pd.DataFrame({'mean_score': [mean_score]}) 