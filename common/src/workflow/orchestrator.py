import yaml
from importlib import import_module
import pandas as pd
import os

class Orchestrator:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        self.pipeline = None
        self.dataloader_setup_model = {}

    def _import_class(self, dotted_path):
        module_path, class_name = dotted_path.rsplit('.', 1)
        module = import_module(module_path)
        return getattr(module, class_name)

    def setup_pipeline(self):
        # Instantiate preprocessors
        preprocessors = []
        for preproc_conf in self.config.get('preprocessors', []):
            PreprocClass = self._import_class(preproc_conf['class'])
            preprocessors.append(PreprocClass(**preproc_conf.get('params', {})))

        # Get dataloader args from model config if present
        model_conf = self.config['model']
        self.dataloader_setup_model = model_conf.get('dataloader_args', {})

        # Instantiate dataloader
        DataloaderClass = self._import_class(self.config['dataloader']['class'])
        dataloader_parameters = self.config['dataloader'].get('params', {})
        dataloader_parameters['preprocessors'] = preprocessors
        dataloader = DataloaderClass(**dataloader_parameters)


        # Instantiate model
        ModelClass = self._import_class(model_conf['class'])
        split = model_conf.get('split', 'train')
        model_params = model_conf.get('params', {})
        model = ModelClass(dataloader, split=split, **model_params)

        # Instantiate evaluator
        EvaluatorClass = self._import_class(self.config['evaluator']['class'])
        evaluator = EvaluatorClass(None, dataloader)  # predictions set after model.predict

        self.pipeline = {
            'preprocessors': preprocessors,
            'dataloader': dataloader,
            'model': model,
            'evaluator': evaluator
        }

    def validate_dataframe_with_yaml(self, df, yaml_path, model_name):
        with open(yaml_path, 'r') as f:
            schema = yaml.safe_load(f)
        model_schema = schema[model_name]
        if model_schema['type'] != 'dataframe':
            raise ValueError(f"Schema type {model_schema['type']} not supported for DataFrame validation.")
        expected_cols = model_schema['columns']
        for col, typ in expected_cols.items():
            if col not in df.columns:
                raise ValueError(f"Missing column: {col}")
            series = df[col]
            if isinstance(typ, dict):
                if typ['type'] == 'list':
                    for idx, val in series.items():
                        if not isinstance(val, list):
                            raise TypeError(f"Row {idx} column {col} is not a list")
                        if val and not all(isinstance(x, self._str_to_type(typ['items'])) for x in val):
                            raise TypeError(f"Row {idx} column {col} list elements are not of type {typ['items']}")
                elif typ['type'] == 'dict':
                    for idx, val in series.items():
                        if not isinstance(val, dict):
                            raise TypeError(f"Row {idx} column {col} is not a dict")
                        if val and not all(isinstance(x, self._str_to_type(typ['values'])) for x in val.values()):
                            raise TypeError(f"Row {idx} column {col} dict values are not of type {typ['values']}")
                else:
                    raise ValueError(f"Unsupported nested type: {typ['type']}")
            else:
                # Check type by pandas dtype
                if typ == 'int' and not pd.api.types.is_integer_dtype(series):
                    raise TypeError(f"Column {col} is not of type int")
                if typ == 'float' and not pd.api.types.is_float_dtype(series):
                    raise TypeError(f"Column {col} is not of type float")
                if typ == 'str' and not pd.api.types.is_string_dtype(series):
                    raise TypeError(f"Column {col} is not of type str")
        print(f"DataFrame validated against YAML schema: {yaml_path}")

    def _str_to_type(self, type_str):
        # Helper to convert type name string to Python type
        return {'int': int, 'float': float, 'str': str, 'Any': object}.get(type_str, object)

    def run(self):
        model = self.pipeline['model']
        dataloader = self.pipeline['dataloader']
        evaluator = self.pipeline['evaluator']
        # Validate dataloader output
        if hasattr(dataloader, 'get_data'):
            data = dataloader.get_data()
            if isinstance(data, pd.DataFrame):
                yaml_path = self.config['dataloader'].get('output_schema')
                if yaml_path and os.path.exists(yaml_path):
                    self.validate_dataframe_with_yaml(data, yaml_path, os.path.splitext(os.path.basename(yaml_path))[0])
        # Validate preprocessor output (if any)
        for idx, preproc in enumerate(self.pipeline['preprocessors']):
            if hasattr(preproc, 'output_data'):
                pdata = preproc.output_data
                yaml_path = self.config['preprocessors'][idx].get('output_schema')
                if yaml_path and os.path.exists(yaml_path) and isinstance(pdata, pd.DataFrame):
                    self.validate_dataframe_with_yaml(pdata, yaml_path, os.path.splitext(os.path.basename(yaml_path))[0])
        # Validate model output (predictions)
        
        dataloader.setup(**self.dataloader_setup_model)
        model.train()
        predictions = model.predict()
        model_output_schema = self.config['model'].get('output_schema')
        if model_output_schema and os.path.exists(model_output_schema) and isinstance(predictions, pd.DataFrame):
            self.validate_dataframe_with_yaml(predictions, model_output_schema, os.path.splitext(os.path.basename(model_output_schema))[0])
        evaluator.predictions = predictions
        result = evaluator.evaluate()
        print('Evaluation result:', result) 