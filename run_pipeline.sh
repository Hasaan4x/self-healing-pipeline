#!/bin/bash
set -euo pipefail
cd /Users/raohasaan/Desktop/self-healing-pipeline
source .venv/bin/activate
python -m app.run_scheduled >> /Users/raohasaan/Desktop/self-healing-pipeline/logs/pipeline.log 2>&1
