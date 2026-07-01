#!/bin/sh
set -e
python seed.py
exec python simulate.py
