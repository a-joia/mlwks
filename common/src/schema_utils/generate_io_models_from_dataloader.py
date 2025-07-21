import importlib
import pandas as pd
from pydantic import BaseModel, ValidationError, create_model
from typing import Any, Dict, Type
import sys
import os
import logging
import json
import yaml
import collections.abc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# set to DEBUG for more verbosity
logger.setLevel(logging.DEBUG)

# NOTE: YAML schema is the source of truth for runtime validation.
# Python class is generated for developer convenience and static typing.
def infer_pydantic_type(dtype) -> Any:
    if pd.api.types.is_integer_dtype(dtype):
        return int
    elif pd.api.types.is_float_dtype(dtype):
        return float
    elif pd.api.types.is_bool_dtype(dtype):
        return bool
    else:
        return str

def infer_yaml_type(series):
    # Try to infer if the column is a list or dict
    sample = series.dropna().iloc[0] if not series.dropna().empty else None
    if isinstance(sample, list):
        # Infer type of list elements
        if sample:
            elem_type = type(sample[0]).__name__
        else:
            elem_type = 'Any'
        return {'type': 'list', 'items': elem_type}
    elif isinstance(sample, dict):
        # Infer type of dict values (assume all same type)
        if sample:
            value_type = type(next(iter(sample.values()))).__name__
        else:
            value_type = 'Any'
        return {'type': 'dict', 'values': value_type}
    else:
        return str(infer_pydantic_type(series.dtype).__name__)

def generate_pydantic_model_from_df(df: pd.DataFrame, model_name: str) -> Type[BaseModel]:

    if isinstance(df, pd.DataFrame):
        fields: Dict[str, tuple] = {}
        for col in df.columns:
            fields[col] = (infer_pydantic_type(df[col].dtype), ...)
        return create_model(model_name, **fields)
    elif isinstance(df, dict):

        return create_model(model_name, **{k: (Any, ...) for k in df.keys()})
    elif isinstance(df, (int, float, str, bool)):
        return create_model(model_name, value=(type(df), ...))
    elif isinstance(df, collections.abc.Iterable):
        # Handle iterable case
        return create_model(model_name, **{str(i): (Any, ...) for i in range(len(df))})

def save_model_code(model: Type[BaseModel], output_path: str):
    code = f"from pydantic import BaseModel\n\nclass {model.__name__}(BaseModel):\n"
    for name, field in model.model_fields.items():
        type_name = field.annotation.__name__ if hasattr(field.annotation, "__name__") else str(field.annotation)
        code += f"    {name}: {type_name}\n"
    with open(output_path, 'w') as f:
        f.write(code)
    logger.info(f"Model code saved to {output_path}")

def generate_yaml_schema_from_df(df: pd.DataFrame, model_name: str, output_path: str):
    logger.debug(f"Generating YAML schema for model: {model_name}")
    if isinstance(df, pd.DataFrame):
        schema = {
            model_name: {
                'type': 'dataframe',
                'columns': {col: infer_yaml_type(df[col]) for col in df.columns}
            }
        }
    elif isinstance(df, dict):
        schema = {
            model_name: {
                'type': 'dict',
                'values': { k: type(v).__name__ for k, v in df.items() }
            }
        }
    elif isinstance(df, collections.abc.Iterable):
        schema = {
            model_name: {
                'type': 'list',
                'items': 'Any'
            }
        }
    elif isinstance(df, (int, float, str, bool)):
        schema = {
            model_name: {
                'type': type(df).__name__
            }
        }
    else:
        raise ValueError("Unsupported data type for schema generation")
    
    logger.debug(f"Saving YAML schema to: {output_path}")
    with open(output_path, 'w') as f:
        yaml.dump(schema, f)
    logger.info(f"YAML schema saved to {output_path}")

def validate_input(input_data: dict, input_model: Type[BaseModel]):
    try:
        return input_model(**input_data)
    except ValidationError as e:
        logger.error(f"Input validation error: {e}")
        sys.exit(1)

def validate_output(df:Any, output_model: Type[BaseModel]):
    if output_model is None:
        logger.warning("No output model provided for validation.")
        return
    # logger.info(f"Validating output data against model: {output_model.__name__}")

    if  df is None:
        logger.warning("No output data provided for validation.")
        return

    if isinstance(df, pd.DataFrame):
        for idx, row in df.iterrows():
            try:
                output_model(**row.to_dict())
            except ValidationError as e:
                logger.error(f"Output validation error in row {idx}: {e}")



def generate_io_models_from_dataloader(dataloader_module: str, dataloader_class_name: str, parameters: dict, output_model_path: str):
    module = importlib.import_module(dataloader_module)
    DataloaderClass = getattr(module, dataloader_class_name)

    logger.info(f"Using dataloader class: {dataloader_class_name} from module: {dataloader_module}")
    # Validate input
    input_obj = validate_input(parameters, DataloaderClass)

    # Get data
    dataloader = DataloaderClass(**parameters)
    df = dataloader.get_data()

    # logger.info(f"Data loaded with shape: {df.shape}")

    # Generate and save output model
    # OutputModel = generate_pydantic_model_from_df(df, DataloaderClass)
    # save_model_code(OutputModel, output_model_path)
    # Generate and save YAML schema in the same directory as the output model
    yaml_output_path = os.path.join(os.path.dirname(output_model_path), os.path.splitext(os.path.basename(output_model_path))[0] + ".yaml")
    # yaml_output_path = output_model_path
    generate_yaml_schema_from_df(df, dataloader_class_name, yaml_output_path)
    # Validate output
    # validate_output(df, yaml_output_path)
    logger.info("Validation complete.")


if __name__ == "__main__":
    # Usage: python generate_io_models_from_dataloader.py <dataloader_module> <dataloader_class> <input_json_file> <output_model_path>
    if len(sys.argv) < 5:
        print("Usage: python generate_io_models_from_dataloader.py <dataloader_module> <dataloader_class> <input_json_file> <output_model_path>")
        print("Example: python generate_io_models_from_dataloader.py shared.dataloaders.my_csv_dataloader MyCsvDataloader input.json MyDataloaderOutput.py")
        sys.exit(1)
    dataloader_module = sys.argv[1]  # e.g., "shared.dataloaders.my_csv_dataloader"
    dataloader_class_name = sys.argv[2]  # e.g., "MyCsvDataloader"
    input_json_file = sys.argv[3]    # e.g., 'input.json'
    output_model_path = sys.argv[4]

    with open(input_json_file, 'r') as f:
        input_dict = json.load(f)

    module = importlib.import_module(dataloader_module)
    DataloaderClass = getattr(module, dataloader_class_name)
    # Fix: InputModel should be <ClassName>Input, not lowercased
    InputModel = getattr(module, f"{dataloader_class_name}Input")

    # Validate input
    input_obj = validate_input(input_dict, InputModel)

    # Get data
    dataloader = DataloaderClass()
    df = dataloader.get_data(input_obj)

    # Generate and save output model
    # OutputModel = generate_pydantic_model_from_df(df, DataloaderClass)
    # save_model_code(OutputModel, output_model_path)
    # Generate and save YAML schema in the same directory as the output model
    yaml_output_path = os.path.join(os.path.dirname(output_model_path), os.path.splitext(os.path.basename(output_model_path))[0] + ".yaml")
    generate_yaml_schema_from_df(df, DataloaderClass, yaml_output_path)
    # Validate output
    # validate_output(df, OutputModel)
    logger.info("Validation complete.") 