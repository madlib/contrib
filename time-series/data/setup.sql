CREATE SCHEMA timeseries;
DROP TABLE IF EXISTS timeseries.moving_average_data;
CREATE TABLE timeseries.moving_average_data
(
  dt TIMESTAMP
, region text
, revenue numeric
) DISTRIBUTED BY (dt, region)
;

\copy timeseries.moving_average_data FROM fake_data.csv WITH CSV HEADER
