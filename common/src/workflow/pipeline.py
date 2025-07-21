# Example pipeline using the ML workflow base classes
from common.src.dataloader.base_dataloader import BaseDataloader
from common.src.preprocessor.base_preprocessor import BasePreprocessor
from common.src.model.base_model import BaseModel
from common.src.evaluator.base_evaluator import BaseEvaluator

# User would implement these classes for their use case
class MyDataloader(BaseDataloader):
    def __init__(self, preprocessors=None, custom_arg=None):
        super().__init__(preprocessors=preprocessors)
        self.custom_arg = custom_arg
    def load_data(self, split: str):
        print(f"MyDataloader received custom_arg: {self.custom_arg}")
        # Load data for the given split
        if split == "train":
            self.data = [1, 2, 3, 4, 5]
        elif split == "val":
            self.data = [10, 20]
        else:
            self.data = []

class MyPreprocessor(BasePreprocessor):
    def process(self, data):
        return data  # No-op

class MyModel(BaseModel):
    def _train(self):
        print(f"Training on data: {list(self.dataloader)}")
    def _predict(self):
        return [x * 2 for x in self.dataloader]

class MyEvaluator(BaseEvaluator):
    def evaluate(self):
        print(f"Evaluating predictions: {self.predictions}")
        return {'accuracy': 1.0}

if __name__ == "__main__":
    preprocessors = [MyPreprocessor()]
    dataloader = MyDataloader(preprocessors=preprocessors, custom_arg=42)
    model = MyModel(dataloader=dataloader, split="train")
    model.train()
    predictions = model.predict(split="val")
    evaluator = MyEvaluator(predictions, dataloader)
    result = evaluator.evaluate()
    print("Evaluation result:", result) 