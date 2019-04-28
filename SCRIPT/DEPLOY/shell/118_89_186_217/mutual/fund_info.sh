#!/bin/sh  
export PYTHONPATH="$PYTHONPATH:/home/yusy/IndexCalculation";
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/sync/base_mutual/fund_info.py';

/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/sync/base_mutual/fund_info_structured.py';

/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/sync/base_mutual/fund_description.py';

/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/sync/base_mutual/fund_fee.py';

/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/sync/base_mutual/fund_holder.py';
