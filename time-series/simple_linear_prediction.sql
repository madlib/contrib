/*

Owner: buddha314 / Brian Dolan / brian@discovix.com

The baseline data is constructed in the data/ directory and includes a table named  timeseries.moving_average_data

See Chatfield: An Introduction To Time Series Analysis for a more complete description of local smoothing.

 */

/* Make a few tables to hold the intermediate stages */
DROP TABLE IF EXISTS timeseries.parameters;
CREATE TABLE  timeseries.parameters
(
  model_id int
, region text
, m numeric
, b numeric
) --DISTRIBUTED BY (model_id)
;

DROP TABLE IF EXISTS timeseries.forecast_window;
CREATE TABLE timeseries.forecast_window
(
  day_ahead int
) --DISTRIBUTED BY (day_ahead)
;

\set days_ahead 7
INSERT INTO timeseries.forecast_window (day_ahead)
SELECT generate_series(1,:days_ahead) AS day_ahead
;


/* Create a place to drop the forecasts when they've been developed */
DROP TABLE IF EXISTS timeseries.forecasts;
CREATE TABLE timeseries.forecasts
(
  region text
, date_id integer
, forecast numeric
) -- DISTRIBUTED BY (region, date_id)
;
 


/* Defined a set of parameters */
\set lg 7

INSERT INTO  timeseries.parameters (model_id, region, m, b)
SELECT
  1::integer AS model_id
, region
, regr_slope(revenue, days_ago) AS m
, regr_intercept(revenue, days_ago) AS b
FROM
  (
  /*  This level contains the raw data, but also normalizes the dates to start at 0 */
  SELECT
    dt::date AS dt
  , (max(dt) OVER (PARTITION BY region))::date  - dt::date  AS days_ago
  , region
  , revenue
  FROM timeseries.moving_average_data mad
  ) AS a
WHERE days_ago >= :lg 
GROUP BY region
;


/* Do the actual forecasts */
INSERT INTO  timeseries.forecasts (region, date_id, forecast)
SELECT
  p.region
, fw.day_ahead
, p.m * fw.day_ahead + p.b AS forecast
FROM  timeseries.parameters p, timeseries.forecast_window fw
;




