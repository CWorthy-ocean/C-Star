#!/bin/bash
python -m cstar.scripts.runner.worker "$@"
# python -m cstar.entrypoint.worker.worker \
#     --log-level DEBUG \
#     --blueprint-uri /opt/blueprint.yml \
#     --output-dir /opt/workeroutput