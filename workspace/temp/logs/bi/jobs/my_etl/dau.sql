SELECT
  abs((DAU - previous_dau)/ previous_dau) > 0.003 as raise_flag,
  *,
  "my-bi-project-ppltx.panels.daily_user_panel" as table_name
--  DAU / d1 > 1.1 as raise_flag
FROM
-- my-bi-project-ppltx.panels.daily_user_panel
(

SELECT
  dt,
  DAU,
  LAG(DAU,1 ) over (order by dt) as previous_dau
FROM
(
SELECT
  dt,
  count(1) as DAU
FROM
`my-bi-project-ppltx.panels.daily_user_panel`
group by all
order by 1 desc
limit 5
)

)
order by dt desc
limit 1