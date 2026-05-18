#!/bin/bash

uv run python sampling_scripts/subsample_annotations.py \
    --species "Phylloscopus trochilus" "Numenius arquata" "Hirundo rustica" \
        "Turdus philomelos" "Erithacus rubecula" \
    --sites cdfb50bf d88bc03a 7e4b3d4f \
    --samples-per-bin 50 \
    --bin-size 0.1 \
    --stratify-by-species \
    --user-ids pierre \
    --output-path validation_dataset/pour_pierre.parquet \
    --diagnostics


#     --stratify-by-device \
