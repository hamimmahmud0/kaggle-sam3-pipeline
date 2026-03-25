#!/usr/bin/env bash
set -euo pipefail
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
export HF_TOKEN="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"
exec /kaggle/working/miniforge3/envs/sam3/bin/python /kaggle/working/SAM3/sam3_remote_pipeline.py "$@"
