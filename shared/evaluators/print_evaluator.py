from common.src.evaluator.base_evaluator import BaseEvaluator

class PrintEvaluator(BaseEvaluator):
    def evaluate(self):
        print('Predictions:', self.predictions)
        return {'dummy_metric': 1.0} 