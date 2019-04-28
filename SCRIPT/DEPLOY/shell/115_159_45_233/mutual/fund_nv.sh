#!/bin/sh  
export PYTHONPATH="$PYTHONPATH:/home/yusy/IndexCalculation";
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/sync/base_mutual/fund_nv_source.py';

/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/sync/base_mutual/fund_nv.py';

/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/sync/base_mutual/fund_yield.py';
