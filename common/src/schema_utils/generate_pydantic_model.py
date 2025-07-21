import pandas as pd
from pydantic import BaseModel, ValidationError, create_model
from typing import Any, Dict, Type
import sys
import os


def infer_pydantic_type(dtype) -> Any:
    if pd.api.types.is_integer_dtype(dtype):
        return int
    elif pd.api.types.is_float_dtype(dtype):
        return float
    elif pd.api.types.is_bool_dtype(dtype):
        return bool
    else:
        return str

def generate_pydantic_model_from_df(df: pd.DataFrame, model_name: str = "DataloaderOutput") -> Type[BaseModel]:
    fields: Dict[str, tuple] = {}
    for col in df.columns:
        fields[col] = (infer_pydantic_type(df[col].dtype), ...)
    return create_model(model_name, **fields)

def save_model_code(model: Type[BaseModel], output_path: str):
    code = f"from pydantic import BaseModel\n\nclass {model.__name__}(BaseModel):\n"
    for name, field in model.model_fields.items():
        type_name = field.annotation.__name__ if hasattr(field.annotation, "__name__") else str(field.annotation)
        code += f"    {name}: {type_name}\n"
    with open(output_path, 'w') as f:
        f.write(code)
    print(f"Model code saved to {output_path}")

def validate_dataframe(df: pd.DataFrame, model: Type[BaseModel]):
    for idx, row in df.iterrows():
        try:
            model(**row.to_dict())
        except ValidationError as e:
            print(f"Validation error in row {idx}: {row.to_dict()}")
            print(e)

if __name__ == "__main__":
    # Example usage: python generate_pydantic_model.py path/to/data.csv output_model.py
    if len(sys.argv) < 3:
        print("Usage: python generate_pydantic_model.py <csv_path> <output_model_path>")
        sys.exit(1)
    csv_path = sys.argv[1]
    output_model_path = sys.argv[2]
    df = pd.read_csv(csv_path)
    model = generate_pydantic_model_from_df(df)
    save_model_code(model, output_model_path)
    print("Validating DataFrame against generated model...")
    validate_dataframe(df, model)
    print("Validation complete.") 