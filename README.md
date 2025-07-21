# ML Workflow Docker Usage

## Build the Docker image
```
docker build -t ml-workflow .
```

## Run schema generation only
```
docker run --rm ml-workflow --generate-schemas
```

## Run the pipeline (with default config)
```
docker run --rm ml-workflow --run-pipeline
```

## Run the pipeline with a specific config file
```
docker run --rm -v $(pwd)/my_config.yml:/app/my_config.yml -v $(pwd)/outputs:/app/outputs ml-workflow --run-pipeline --config my_config.yml
```

You can combine schema generation and pipeline run:
```
docker run --rm -v $(pwd)/my_config.yml:/app/my_config.yml -v $(pwd)/outputs:/app/outputs ml-orchestrator --generate-schemas --run-pipeline --config my_config.yml
```

# Notes
- The `--config` argument lets you specify any pipeline configuration YAML file.
- The `-v $(pwd)/my_config.yml:/app/my_config.yml` mounts your config file into the container.
- The `-v $(pwd)/outputs:/app/outputs` ensures outputs are accessible on your host. 



docker run --rm -v C:/Users/Ajoia/Workspace/ml_workflow/my_config.yml:/app/my_config.yml -v C:/Users/Ajoia/Workspace/ml_workflow/outputs:/app/outputs ml-orchestrator --generate-schemas --run-pipeline --config my_config.yml


docker run --rm -v C:/Users/Ajoia/Workspace/ml_workflow/pipelines:/app/pipelines -v C:/Users/Ajoia/Workspace/ml_workflow/outputs:/app/outputs ml_orchestrator --generate-schemas --run-pipeline --config /app/pipelinestest_pipelineexample_config.yml