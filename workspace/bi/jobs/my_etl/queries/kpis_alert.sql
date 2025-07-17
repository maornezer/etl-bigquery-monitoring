/*
exec time
{run_time}
{description}
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
  abs((DAU - d1)/ d1) > {thresh_in_percent} as raise_flag,
  *,
  "{project}.{dataset}.{table_id}" as table_name
--  DAU / d1 > 1.1 as raise_flag
FROM
-- {project}.{dataset}.{table_id}
(

SELECT
  dt,
  DAU,
  LAG(DAU,1 ) over (order by dt) as d1
FROM dau_table
)
order by dt desc
limit 1