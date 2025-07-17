/*
exec time
{run_time}
{description}
{file_name}
*/

SELECT
    datetime_diff(current_datetime(), ts, hour) > {thresh_in_hours} as raise_flag,
    FORMAT_DATE('%Y-%m-%d %H',ts) AS lt,
    *
FROM
--  `{project}.logs.log_*`
  `{project}.logs.{job_type}_logs`
WHERE
  TRUE
  AND job_name = '{job_name}'
  AND file_name = '{file_name}'
  AND step_name = '{step_name}'
  {enable_message} AND message like '{message}'
ORDER BY
  ts desc
  Limit 1