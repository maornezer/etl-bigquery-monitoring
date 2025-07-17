/*
exec time
2025-07-15 20:25:12.545548
Check that the KPI hadn't change dramatically
*/




with player_dau as (
SELECT * FROM
(
SELECT
    substring(generate_uuid(),0,8) AS uid,
    current_date() as dt,
FROM UNNEST(GENERATE_ARRAY(1, 100))
UNION ALL
SELECT
    substring(generate_uuid(),0,8) AS uid,
    date_sub(current_date(), interval 1 day) as dt,
FROM UNNEST(GENERATE_ARRAY(1, 80))
UNION ALL
SELECT
    substring(generate_uuid(),0,8) AS uid,
    date_sub(current_date(), interval 2 day) as dt,
FROM UNNEST(GENERATE_ARRAY(1, 90))
)
),

dau_table as
(
SELECT
dt,
count(1) as DAU
from player_dau
group by 1
)


SELECT
  abs((DAU - d1)/ d1) > 0.15 as raise_flag,
  *,
  "my-bi-project-ppltx.logs.daily_logs" as table_name
--  DAU / d1 > 1.1 as raise_flag
FROM
-- my-bi-project-ppltx.logs.daily_logs
(

SELECT
  dt,
  DAU,
  LAG(DAU,1 ) over (order by dt) as d1
FROM dau_table
)
order by dt desc
limit 1