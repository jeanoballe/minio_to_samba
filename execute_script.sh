#!/bin/sh
cd "$(dirname "$0")";
CWD="$(pwd)"
echo $CWD
python3 minio_to_samba.py