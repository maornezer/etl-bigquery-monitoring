#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ref
https://cloud.google.com/bigquery/docs/samples/bigquery-get-table


python ./jobs/my_etl/table_validation.py my-bi-project-ppltx --etl-name dataset --etl-action daily --days-back 2

"""
from pathlib import Path
import os
import re
import sys
from datetime import datetime, timedelta, date, timezone
from google.cloud import bigquery
import argparse
import uuid
import platform
import pandas as pd

# adapt the env to Mac or windows
if os.name == 'nt':
    home = Path("C:/workspace/")
else:
    home = Path(os.path.expanduser("~/workspace/"))

# get repository name
repo_name = re.search(r"(.*)[/\\]workspace[/\\]([^/\\]+)", __file__).group(2)
repo_tail = re.search(r".*[/\\]" + repo_name + r"[/\\](.+)[/\\]", __file__).group(1)
sys.path.insert(0, str(home / f"{repo_name}/utilities/"))

from my_etl_files import readJsonFile, ensureDirectory, writeFile, readFile, header, get_paths
from df_to_string_table import format_dataframe_for_slack

bi_path, bi_auth, data_path, logs_path, folder_name = get_paths(repo_name, home, __file__, repo_tail)

def process_command_line(argv):
    if argv is None:
        argv = sys.argv[1:]
    # initialize the parser object:

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("project_id", choices=["my-bi-project-ppltx"],
                        help="""Operation to perform. The arguments for each option are:
                        Full_Load:   --date""",
                        default="ppltx-ba-course")
    parser.add_argument("--etl-action", choices=["init", "daily", "delete"], help="""Action etl job""")
    parser.add_argument("--etl-name", help="""The name of the etl job""")
    parser.add_argument("--dry-run", help="""if True don't execute the queries""", action="store_true")
    parser.add_argument("--days-back", help="""The number of days we want to go back""",
                        default=0)

    return parser, argparse.Namespace()


parser, flags = process_command_line(sys.argv[1:])
x = sys.argv[1:]
parser.parse_args(x, namespace=flags)

# define the project_id
project_id = flags.project_id
etl_name = flags.etl_name
etl_action = flags.etl_action
days_back = int(flags.days_back)

# Construct a BigQuery client object.
client = bigquery.Client(project=project_id)

# Get dates
date_today = date.today()
run_time = datetime.now()
if not run_time.tzinfo:
    run_time = run_time.replace(tzinfo=timezone.utc)
y_m_d = (date_today + timedelta(days=-days_back)).strftime("%Y-%m-%d")
ymd = y_m_d.replace("-", "")

step_id = 0
env_type = 'daily'
log_table = f"{project_id}.logs.daily_logs"

ensureDirectory(logs_path)
ensureDirectory(data_path)


# init log dict
log_dict = {'ts': datetime.now(),
            'dt': datetime.now().strftime("%Y-%m-%d"),
            'uid': str(uuid.uuid4())[:8],
            'username': platform.node(),
            'job_name': etl_name,
            'job_type': etl_action,
            'file_name': os.path.basename(__file__),
            'step_name': 'start',
            'step_id': step_id,
            'log_type': env_type,
            'message': str(x)
            }


# functions
def set_log(log_dict, step, log_table=log_table):
    log_dict['step_name'] = step
    log_dict['step_id'] += 1
    log_dict['ts'] = datetime.now()
    log_dict['dt'] = datetime.now().strftime("%Y-%m-%d")
    job = client.load_table_from_dataframe(pd.DataFrame(log_dict, index=[0]), log_table)
    job.result()  # Wait for the job to complete.


if not flags.dry_run:
    set_log(log_dict, "start")

# get dataset configuration
etl_configuration = readJsonFile(folder_name / f"config/{etl_name}_config.json")

# dictionary for queries
query_dict = {}
alert_columns ='raise_flag'

tables_dict = {"table_name": [],
               "dataset": [],
               "last_update": [],
               "rows": [],
               "bytes": [],
               "hours_diff": [],
               "alert_threshold": []
               }

# Iterate all the datasets groups in the conf
for dataset_name, tables_conf in etl_configuration.items():
    header(dataset_name)

    conf = tables_conf["tables"]

    for table_name, table_conf in conf.items():
        print(table_name)
        table_id = f'{project_id}.{dataset_name}.{table_name}'
        table = client.get_table(table_id) #  Make an API request
        print("Table modified at {}".format(table.modified.strftime("%Y-%m-%d %H")))

        # Collect all the attributes of the table
        tables_dict["table_name"].append(table.table_id)
        tables_dict["dataset"].append(dataset_name)
        table_modified = table.modified
        tables_dict["last_update"].append(table_modified.strftime("%Y-%m-%d %H"))
        tables_dict["rows"].append(table.num_rows)
        tables_dict["bytes"].append(table.num_bytes)

        if not table_modified.tzinfo:
            table_modified = table_modified.replace(tzinfo=timezone.utc)
        tables_dict["hours_diff"].append(round((run_time - table_modified).total_seconds() / 3600))
        tables_dict["alert_threshold"].append(table_conf)

# Transform the dictionary to Dataframe
df_tables = pd.DataFrame(tables_dict)
writeFile(logs_path / f"{etl_name}_tables_validation.csv", df_tables.to_csv(index=False) )

# check for alerts
df_alerts = df_tables[ df_tables['hours_diff'] > df_tables['alert_threshold'] ]

if not df_alerts.empty:
    error_msg = "One or more tables are not updated"
    msg = (f"\n\n*Data Validation Alert*\n[tables_validation.py]\n*source dataset:* _`{project_id}`_\n\n{error_msg}.\n\n*results table:*\n\n" + format_dataframe_for_slack(
            df_alerts))
    # s3_slack.update_with_slack_message(msg)
    writeFile(logs_path / "tables_alert_slack_msg.md", msg)
    print(msg)



if not flags.dry_run:
    set_log(log_dict, "end")
