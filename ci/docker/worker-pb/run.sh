#!/bin/bash
python -m cstar.scripts.runner.worker "$@"
# python -m cstar.scripts.runner.worker \
#     --log-level DEBUG \
#     --blueprint-uri /blueprints/blueprint.yml \
#     --output-dir /output/workeroutput
