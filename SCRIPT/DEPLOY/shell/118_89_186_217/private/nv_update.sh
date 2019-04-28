#!/bin/sh  
export PYTHONPATH="$PYTHONPATH:/home/yusy/IndexCalculation";
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/DataQuality/fund_nv_data_source/nv_data_source.py';
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/DataQuality/fund_nv_data_standard/swanav.py';
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/DataQuality/nv2std.py';
