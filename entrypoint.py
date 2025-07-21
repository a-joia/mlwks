import argparse
import subprocess
import sys
import os
from datetime import datetime
from venv import logger
import yaml
from common.src.schema_utils import generate_io_models_from_dataloader as gimfd

def get_output_dir(config_path):
    dt = datetime.now().strftime('%Y%m%d_%H%M%S')
    config_base = os.path.splitext(os.path.basename(config_path))[0]
    outdir = os.path.join('outputs', f"{config_base}_{dt}")
    # os.makedirs(outdir, exist_ok=True)
    return outdir

def generate_schemas(output_dir, config_path):
    print(f"Generating dataloader output schema in {output_dir}...")
    env = os.environ.copy()
    env["PYTHONPATH"] = "/app"
    # Parse config to get dataloader class and input
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    print(config)

    dataloader_class_path = config['dataloader']['class']
    # Correct extraction: module is all but last, class is last
    dataloader_module = '.'.join(dataloader_class_path.split('.')[:-1])
    dataloader_class_name = dataloader_class_path.split('.')[-1]
    dataloader_params = config['dataloader'].get('params', {})  # Default to input.json if not specified
    output_path = config['dataloader'].get('output_schema',"")
    generate_schema_flag = config['dataloader'].get('generate_schema', True)
    if not generate_schema_flag:
        print("Skipping schema generation as generate_schema is set to False in config.")
        return
    if not output_path:
        raise ValueError("output_schema must be specified in the config under dataloader.")

    # get dir from output_path and create if not exists

    # output_dirname = os.path.dirname(output_path)

    # if not os.path.exists(output_dirname):
        # os.makedirs(output_dirname)
        # logger.info(f"Created directory for output schema: {output_dirname}")

    
    gimfd.generate_io_models_from_dataloader(
        dataloader_module,
        dataloader_class_name,
        dataloader_params,
        output_path
    )

    print("Schema generation complete.")

def run_pipeline(config_path):
    print(f"Running pipeline with config: {config_path}")
    from common.src.workflow.orchestrator import Orchestrator
    o = Orchestrator(config_path)
    o.setup_pipeline()
    o.run()

def main():
    parser = argparse.ArgumentParser(description="ML Workflow Entrypoint")
    parser.add_argument('--generate-schemas', action='store_true', help='Generate schemas for the pipeline')
    parser.add_argument('--run-pipeline', action='store_true', help='Run the pipeline')
    parser.add_argument('--config', type=str, default='src/workflow/example_config.yml', help='Path to pipeline config')
    args = parser.parse_args()

    print(args)
    output_dir = get_output_dir(args.config)
    print(f"All outputs will be saved in: {output_dir}")

    if args.generate_schemas:
        generate_schemas(output_dir, args.config)
    if args.run_pipeline or not args.generate_schemas:
        run_pipeline(args.config)

if __name__ == "__main__":
    main() 