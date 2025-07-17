
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# table_id = 'your-project.your_dataset.your_table'
table_id = 'my-bi-project-ppltx.logs.daily_logs'

table = client.get_table(table_id)  # Make an API request.

# View table properties
print(f"Got table '{table.project}.{table.dataset_id}.{table.table_id}'")

print("Table schema: {}".format(table.schema))
print("Table description: {}".format(table.description))
print("Table has {} rows".format(table.num_rows))
print("Table modified at {}".format(table.modified.strftime("%Y-%m-%d %H")))