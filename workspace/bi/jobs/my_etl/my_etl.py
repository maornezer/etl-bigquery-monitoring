#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

Reference
* [US states csv file]
(https://storage.googleapis.com/cloud-samples-data/bigquery/us-states/us-states.csv)

* [US states link]
https://console.cloud.google.com/storage/browser/cloud-samples-data/bigquery/us-states?inv=1&invt=Ab2G9g
Run Commands

python ./jobs/my_etl/my_etl.py my-bi-project-ppltx --etl-name us_states_csv --etl-action daily --days-back 2

python ./jobs/my_etl/my_etl.py my-bi-project-ppltx --etl-name animals --etl-action daily --days-back 2
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

# adapt the env to mac or windows
if os.name == 'nt':
    home = Path("C:/workspace/")
else:
    home = Path(os.path.expanduser("~/workspace/"))

# get repository name
repo_name = re.search(r"(.*)[/\\]workspace[/\\]([^/\\]+)", __file__).group(2)
repo_tail = re.search(r".*[/\\]" + repo_name + r"[/\\](.+)[/\\]", __file__).group(1)
sys.path.insert(0, str(home / f"{repo_name}/utilities/"))

from my_etl_files import readJsonFile, ensureDirectory, writeFile, readFile


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
                        default=1)

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

# get etl  configuration
etl_configuration = readJsonFile(home / repo_name / repo_tail / f"config/{etl_name}_config.json")

# Iterate all the files in the conf
for etl_name, etl_conf in etl_configuration.items():
    if not etl_conf["isEnable"]:
        continue
    print(f"{etl_name}\n{len(etl_name) * '='}\n")

    print("Extract")
    if not flags.dry_run:
        set_log(log_dict, f"Extract {etl_name}")
    # Define the API endpoint and any necessary headers (if required)
    api_url = etl_conf["url"]


    # api_url = "https://storage.googleapis.com/cloud-samples-data/bigquery/us-states/us-states.csv"
    # headers = {
    #     "Authorization": "Bearer YOUR_API_KEY",  # Use if an API key or token is required
    #     "Content-Type": "application/json"
    # }

    # Function to extract data from the API
    def extract_data_from_api(url, headers=None):
        try:
            # Send a GET request to the API
            response = requests.get(url, headers=headers)

            # Check if the response is successful (status code 200)
            if response.status_code == 200:
                data = response.text
                return data
            else:
                # Handle non-successful response
                error_msg = f"Failed to retrieve data: {response.status_code} - {response.text}"
                print(error_msg)

                if not flags.dry_run:
                    log_dict["message"] = error_msg
                    set_log(log_dict, f"Error in Extract {etl_name}")
                    log_dict["message"] = str(x)
                return None

        except requests.exceptions.RequestException as e:
            # Handle any request-related errors
            error_msg = f"An error occurred:{e}"
            print(error_msg)
            if not flags.dry_run:
                log_dict["message"] = error_msg
                set_log(log_dict, f"Error in API Extract {etl_name}")
                log_dict["message"] = str(x)
            return None


    # Call the function to extract data
    data = extract_data_from_api(api_url)

    data_folder = home / "temp/data/" / repo_name / repo_tail
    ensureDirectory(data_folder)
    # write data to file
    data_file = data_folder / etl_conf["data_file"]
    writeFile(data_file, data)

    print("Transform")
    if not flags.dry_run:
        set_log(log_dict, "Transform")
    print("Load")

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = f'{project_id}.{etl_conf["table_id"]}_{ymd}'

    job_config = bigquery.LoadJobConfig(
        schema=client.schema_from_json(home / repo_name / repo_tail / f"config/{etl_conf['schema']}"),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # The source format defaults to CSV, so the line below is optional.
        # source_format=bigquery.SourceFormat.CSV
        source_format=etl_conf["source_format"]
    )

    if etl_conf["source_format"] == "CSV":
        job_config.skip_leading_rows = 1
        if etl_conf.get("null_marker"):
            job_config.null_marker = etl_conf.get("null_marker")

    with open(data_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.

    msg = f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}"
    print(msg)
    if not flags.dry_run:
        log_dict["message"] = msg
        set_log(log_dict, f"Load finished {etl_name}")
        log_dict["message"] = str(x)

if not flags.dry_run:
    set_log(log_dict, "end")
