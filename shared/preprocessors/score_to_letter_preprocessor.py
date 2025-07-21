from common.src.preprocessor.base_preprocessor import BasePreprocessor
import pandas as pd

class ScoreToLetterPreprocessor(BasePreprocessor):
    def process(self, data):
        if isinstance(data, pd.DataFrame) and 'score' in data.columns:
            data = data.copy()
            def grade(s):
                if s >= 90:
                    return 'A'
                elif s >= 80:
                    return 'B'
                else:
                    return 'C'
            data['grade'] = data['score'].apply(grade)
        return data 