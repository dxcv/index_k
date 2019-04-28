#!/bin/sh
export PYTHONPATH="$PYTHONPATH:/home/yusy/IndexCalculation";
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/4R/org_return_m.py';
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/4R/org_risk_m.py';
/usr/local/python3/bin/python3.5 '/home/yusy/IndexCalculation/Scripts/4R/org_research_m.py';