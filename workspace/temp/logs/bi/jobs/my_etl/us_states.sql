/*
exec time
2025-07-13 07:09:40.834759
Check that the table had been updated in the recent 24 hours
tables_monitoring.py
*/

SELECT
  timestamp_diff(current_timestamp(),TIMESTAMP_MILLIS(last_modified_time ), hour) > 24 as raise_flag,
  FORMAT_DATE('%Y-%m-%d %H',TIMESTAMP_MILLIS(last_modified_time)) AS lt,
  project_id,
  dataset_id,
  table_id,
  row_count,
FROM
  `my-bi-project-ppltx.my_etl`.__TABLES__
WHERE table_id = 'us_state_json'