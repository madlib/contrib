/*

Owner: buddha314 / Brian Dolan / brian@discovix.com

This code produces a simple moving average, also called local smoothing.

lg:  the number of lag (backward) periods to consider in the smoothing
ld:  the number of lead (forward) periods to consider in the smoothing

Local smoothing is often used in time series to ameliorate the effects of local spikes which cannot or should not be modeled in a time series.  For example, an unusual spike in last Thursday would have less impact on the model if we used the average of Weds, Thurs, Fri instead of the just the Thursday value.  Naturally, if all Thursdays are showing spikes, the model should be adjusted to incorporate this behavior.


See Chatfield: An Introduction To Time Series Analysis for a more complete description of local smoothing.

 */

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

