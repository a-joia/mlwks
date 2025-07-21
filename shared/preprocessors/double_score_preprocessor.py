from common.src.preprocessor.base_preprocessor import BasePreprocessor
import pandas as pd

class DoubleScorePreprocessor(BasePreprocessor):
    def process(self, data):
        if isinstance(data, pd.DataFrame) and 'score' in data.columns:
            data = data.copy()
            data['score'] = data['score'] * 2
        return data 