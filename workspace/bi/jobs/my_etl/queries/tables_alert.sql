/*
exec time
{run_time}
{description}
{file_name}
*/

SELECT
  timestamp_diff(current_timestamp(),TIMESTAMP_MILLIS(last_modified_time ), hour) > {thresh_in_hours} as raise_flag,
  FORMAT_DATE('%Y-%m-%d %H',TIMESTAMP_MILLIS(last_modified_time)) AS lt,
  project_id,
  dataset_id,
  table_id,
  row_count,
FROM
  `{project}.{dataset_id}`.__TABLES__
WHERE table_id = '{table_id}'