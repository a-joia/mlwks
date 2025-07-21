from common.src.evaluator.base_evaluator import BaseEvaluator
import pandas as pd

class ScoreThresholdEvaluator(BaseEvaluator):
    def evaluate(self):
        preds = self.predictions
        if isinstance(preds, pd.DataFrame) and 'score' in preds.columns:
            all_above_90 = (preds['score'] > 90).all()
        else:
            all_above_90 = False
        return {'all_above_90': all_above_90} 