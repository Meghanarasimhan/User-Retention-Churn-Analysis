-- 01_calendar_dimension.sql

CREATE OR REPLACE TABLE `streaming_engagement.calendar_dim` AS
WITH calendar AS (
  SELECT
    day,
    EXTRACT(YEAR FROM day) AS year,
    EXTRACT(MONTH FROM day) AS month,
    EXTRACT(DAY FROM day) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM day) AS day_of_week,
    EXTRACT(WEEK FROM day) AS iso_week,
    EXTRACT(QUARTER FROM day) AS quarter,
    FORMAT_DATE('%Y-%m', day) AS year_month,
    FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(day, MONTH)) AS month_start,
    FORMAT_DATE('%A', day) AS weekday_name,
    CASE WHEN EXTRACT(DAYOFWEEK FROM day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
  FROM UNNEST(GENERATE_DATE_ARRAY('2021-10-30', '2023-06-20')) AS day
)
SELECT * FROM calendar;

-- 02_user_activity_enriched.sql

CREATE OR REPLACE TABLE `streaming_engagement.user_activity_enriched` AS
SELECT
  dcs.profile_name,
  dcs.activity_date,
  dcs.is_active,
  cd.year,
  cd.month,
  cd.day_of_week,
  cd.weekday_name,
  cd.is_weekend,
  cd.iso_week,
  cd.quarter,
  cd.year_month,
  cd.month_start
FROM `streaming_engagement.user_daily_cumulative_state` dcs
LEFT JOIN `streaming_engagement.calendar_dim` cd
  ON dcs.activity_date = cd.day;


-- Activity in weekend v. weekday

SELECT
  is_weekend,
  COUNTIF(is_active) AS active_days,
  COUNT(DISTINCT profile_name) AS unique_users
FROM `streaming_engagement.user_activity_enriched`
GROUP BY is_weekend;

-- Signup trend Analysis 

SELECT
  cohort_month,
  COUNT(*) AS total_signups
FROM `streaming_engagement.user_cohorts`
GROUP BY cohort_month
ORDER BY total_signups DESC;



