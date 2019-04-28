#!/bin/sh  
export PYTHONPATH="$PYTHONPATH:/home/yusy/IndexCalculation";
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/SCRIPT/PRIVATE/etl/fund_info/fund_type.py';
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/DataQuality/fund_info/fund_org.py';
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/DataQuality/fund_info_aggregation.py';
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/DataQuality/timeindex.py';

