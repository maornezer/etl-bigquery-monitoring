
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# dataset_id = 'your-project.your_dataset'
dataset_id = 'my-bi-project-ppltx.my_etl'


tables = client.list_tables(dataset_id)  # Make an API request.

print("Tables contained in '{}':".format(dataset_id))
for table in tables:
    print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))
    table_obj = client.get_table(table)
    print("Table modified at {}".format(table_obj.modified.strftime("%Y-%m-%d %H")))
    print("\n")