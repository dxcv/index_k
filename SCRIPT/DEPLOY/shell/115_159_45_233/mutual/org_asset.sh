#!/bin/sh  
export PYTHONPATH="$PYTHONPATH:/home/yusy/IndexCalculation";
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/sync/base_mutual/org_asset_scale.py';

/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/sync/base_mutual/org_portfolio_asset.py';

/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/sync/base_mutual/org_portfolio_industry.py';

/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/sync/base_mutual/org_position_stock.py';
