#!/bin/sh  
export PYTHONPATH="$PYTHONPATH:/home/yusy/IndexCalculation";
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/sync/base_mutual/fund_dividend.py';

/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/sync/base_mutual/fund_split.py';

/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/sync/base_mutual/fund_dividend_split.py';
