/*
exec time
2025-07-12 15:12:36.963919
Check that the job run in the recent 24 hours
my_etl.py
*/

SELECT
    datetime_diff(current_datetime(), ts, hour) > 24 as raise_flag,
    FORMAT_DATE('%Y-%m-%d %H',ts) AS lt,
    *
FROM
--  `my-bi-project-ppltx.logs.log_*`
  `my-bi-project-ppltx.logs.daily_logs`
WHERE
  TRUE
  AND job_name = 'animals'
  AND file_name = 'my_etl.py'
  AND step_name = 'end'
  -- AND message like ''
ORDER BY
  ts desc
  Limit 1