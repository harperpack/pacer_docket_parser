#!/usr/bin/env bash

python extract.py $1
python transform.py
echo "We are ready to upload parsed dockets in ./parsed_dockets/noacri_dockets."
