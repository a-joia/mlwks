
#  Add python environment
$env:PYTHONPATH ="." # current director

$PIPELINEPATH="pipelines\test_pipeline\test_config_7.yml"


python .\entrypoint.py --generate-schemas --run-pipeline --config $PIPELINEPATH  