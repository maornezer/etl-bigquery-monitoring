#!/bin/bash

# Run the python script according to our configuration
cd ~/workspace/bi/

# bash ~/workspace/bi/jobs/my_etl/scheduler/execute_log_monitoring.sh


python ./jobs/my_etl/logs_monitoring.py my-bi-project-ppltx --etl-name log --etl-action daily --days-back 2
