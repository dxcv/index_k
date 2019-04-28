#!/bin/sh
export PYTHONPATH="$PYTHONPATH:/home/yusy/IndexCalculation";
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/SCRIPT/OTHER/configinit/mod_sync_source.py';
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/SCRIPT/OTHER/configinit/mod_id_match.py';