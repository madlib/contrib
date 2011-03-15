/*

Owner: buddha314 / Brian Dolan / brian@discovix.com

 A common method for time series is Exponential Smoothing.  See http://en.wikipedia.org/wiki/Exponential_smoothing for an overview. It amounts to taking a weighted average, with values further in the past having less weight than recent values.

Some tips:

* Most databases represent dates as integers since some epoch.  In exponential smoothing, the difference in dates will be used as an exponent.  Thus, normalizing to a small integer will prevent underflow.  We do this in the example below.

* It is a good idea to de-trend the model before smoothing, but that may not be possible or useful.

* Time series forecasting involves rolling forecasts.  So today's forecast for Friday is different than tomorrow's forecast for Friday.  Thus, keeping track of both the MODEL date and the FORECAST date is essential.  Our example is designed to run the smoothing based on TODAY and only including the last lg days
Below, we do everything in one pass, which is good pedantically but not practically.

 */
\set lg 7
\set alpha 0.1
SELECT
  b.region
, b.alpha
, b.dt
, b.days_ago
, b.revenue
, b.weighted_revenue
, avg(b.weighted_revenue) OVER (PARTITION BY region) AS revenue_smoothed
FROM
  (
  SELECT
    a.dt AS dt
  , model.alpha
  , model.lg
  , a.revenue
  , a.region
  , days_ago
  , (1 - model.alpha)^a.days_ago * a.revenue AS weighted_revenue
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
  , (
    /* This level generates a model.  In practice, this should be it's own table */
    SELECT
      :alpha::numeric AS alpha
    , :lg::integer AS lg
    ) AS model
  ) AS b
WHERE
  b.days_ago >= b.lg
ORDER BY
  b.region
, b.dt
;

