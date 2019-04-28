#!/bin/sh  
export PYTHONPATH="$PYTHONPATH:/home/yusy/IndexCalculation";
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/DataQuality/org_info/org_scale_range.py';

/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/DataQuality/org_info/master_strategy.py';

/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/DataQuality/org_info/managers.py';

/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/DataQuality/org_info/funds_num.py';

/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/DataQuality/org_info/base_date.py';
