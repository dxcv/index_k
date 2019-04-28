#!/bin/sh  
export PYTHONPATH="$PYTHONPATH:/home/yusy/IndexCalculation";
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/Index/fund_index_all_m.py';
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/Index/fund_index_all_static_m.py';