#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

python ./jobs/my_etl/kpis_monitoring.py my-bi-project-ppltx --etl-name kpis --etl-action daily --days-back 2
"""
from pathlib import Path
import os
import requests
import re
import sys
from datetime import datetime, timedelta, date
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
    parser.add_argument("project_id", choices=["ppltx-ba-course", "ppltx-bi-developer", "my-bi-project-ppltx"],
                        help="""Operation to perform. The arguments for each option are:
                        Full_Load:   --date""",
                        default="ppltx-ba-course")
    parser.add_argument("--etl-action", choices=["init", "daily", "delete"], help="""The action the etl job""")
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

ensureDirectory(logs_path)
ensureDirectory(data_path)

# Construct a BigQuery client object.
client = bigquery.Client(project=project_id)

# Get dates
date_today = date.today()
run_time = datetime.now()
y_m_d = (date_today + timedelta(days=-days_back)).strftime("%Y-%m-%d")
ymd = y_m_d.replace("-", "")

step_id = 0
env_type = 'daily'
log_table = f"{project_id}.logs.daily_logs"

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

# get kpis configuration
kpis_configuration = readJsonFile(folder_name/ f"config/{etl_name}_config.json")

# dictionary for queries
query_dict = {}
alert_columns ='raise_flag'
# Iterate all the validation groups in the conf
for kpis_group_name, group_conf in kpis_configuration.items():
    header(kpis_group_name)

    # query_sql = readFile(home / repo_name / repo_tail / f"queries/logs_alert.sql")
    query_sql = readFile(folder_name / f"queries/{etl_name}_alert2.sql")

    query_params_base = {"date": y_m_d,
                         "run_time": run_time,
                         "project": project_id,
                         "job_type": etl_action}

    for kpis_name, kpis_conf in  group_conf["kpis"].items():
        print(kpis_name)

        if not kpis_conf["isEnable"]:
            continue

        # enriched query params
        query_params = query_params_base
        query_params.update(kpis_conf)

        query = query_sql.format(**query_params)

        # write a query to log

        writeFile(logs_path/ f"{kpis_name}.sql", query)

        if not flags.dry_run:
            try:
                job_id = client.query(query)
                query_df = job_id.to_dataframe()
                query_dict[kpis_name] = {}
                # get job details
                job = client.get_job(job_id)

                # union the query results
                if len(query_dict.keys()) == 1:
                    df_all = query_df
                else:
                    df_all = pd.concat([df_all, query_df], ignore_index=True)
            except Exception as s:
                msg_error = f"The error is {s}"
                header(f"Hi BI Developer we have a problem\nOpen file {str(logs_path)}/error.md")
                print(msg_error)
                writeFile(logs_path / "error.md", msg_error)
                # To send alert in Slack


# if the df has values, send a message
if (df_all[alert_columns]).any():
    # extract only the rows with raising_flag = True
    print(df_all[df_all[alert_columns]])
    error_msg = "[ KPIs Alert]"
    df_alert = df_all[df_all[alert_columns]]
    df_alert = df_alert.loc[:, df_alert.columns != 'message']
    msg = (f"{error_msg}\n\n*There is a significant change in the KPIs*\n" + format_dataframe_for_slack(df_alert)) # query_df.to_string(index=1))
    # slack_obj.update_with_slack_message(msg)
    writeFile(logs_path / f"{etl_name}_monitoring_msg.md", msg)
    print(msg)



if not flags.dry_run:
    set_log(log_dict, "end")
