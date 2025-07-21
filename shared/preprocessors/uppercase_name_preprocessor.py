from common.src.preprocessor.base_preprocessor import BasePreprocessor
import pandas as pd

class UppercaseNamePreprocessor(BasePreprocessor):
    def process(self, data):
        if isinstance(data, pd.DataFrame) and 'name' in data.columns:
            data = data.copy()
            data['name'] = data['name'].str.upper()
        return data 