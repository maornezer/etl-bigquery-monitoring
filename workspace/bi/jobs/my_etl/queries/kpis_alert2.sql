SELECT
  abs((DAU - {d1})/ {d1}) > {thresh_in_percent} as raise_flag,
  *,
  "{project}.{dataset}.{table_id}" as table_name
--  DAU / d1 > 1.1 as raise_flag
FROM
-- {project}.{dataset}.{table_id}
(

SELECT
  dt,
  DAU,
  LAG(DAU,1 ) over (order by dt) as {d1}
FROM
(
SELECT
  dt,
  count(1) as DAU
FROM
`{project}.{dataset}.{table_id}`
group by all
order by 1 desc
limit 5
)

)
order by dt desc
limit 1