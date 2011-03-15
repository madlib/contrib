\set ld 3
\set lg 3
SELECT
  dt
, region
, revenue
, avg(revenue) OVER (twdw) AS moving_average
FROM  timeseries.moving_average_data mad
WINDOW twdw AS (PARTITION BY region ORDER BY dt ROWS BETWEEN :lg PRECEDING AND :ld FOLLOWING)
ORDER BY
  region
, dt
;

