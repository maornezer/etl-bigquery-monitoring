#!/bin/bash

# Run the python script according to our configuration
cd ~/workspace/bi/

# bash ~/workspace/bi/jobs/my_etl/scheduler/execute_my_etl_daily.sh


python ./jobs/my_etl/my_etl.py my-bi-project-ppltx --etl-name animals --etl-action daily --dry-run
python ./jobs/my_etl/my_etl.py my-bi-project-ppltx --etl-name etl --etl-action daily
