-- 01_user_cohorts.sql

CREATE OR REPLACE TABLE `streaming_engagement.user_cohorts` AS
SELECT
  profile_name,
  MIN(activity_date) AS signup_date,
  FORMAT_DATE('%Y-%m', MIN(activity_date)) AS cohort_month
FROM `streaming_engagement.user_daily_cumulative_state`
WHERE is_active = TRUE
GROUP BY profile_name;

-- 02_user_activity_with_cohort.sql

CREATE OR REPLACE TABLE `streaming_engagement.user_activity_with_cohort` AS
SELECT
  dcs.profile_name,
  dcs.activity_date,
  dcs.is_active,
  c.signup_date,
  c.cohort_month,
  DATE_DIFF(dcs.activity_date, c.signup_date, DAY) AS days_since_signup
FROM `streaming_engagement.user_daily_cumulative_state` dcs
JOIN `streaming_engagement.user_cohorts` c
  ON dcs.profile_name = c.profile_name
WHERE dcs.activity_date >= c.signup_date;  -- ignore scaffolded rows before signup

-- 03_cohort_retention_matrix.sql

SELECT
  cohort_month,

  COUNT(DISTINCT CASE WHEN days_since_signup = 0 AND is_active THEN profile_name END) AS signup_day,
  COUNT(DISTINCT CASE WHEN days_since_signup = 1 AND is_active THEN profile_name END) AS day_1,
  COUNT(DISTINCT CASE WHEN days_since_signup = 2 AND is_active THEN profile_name END) AS day_2,
  COUNT(DISTINCT CASE WHEN days_since_signup = 3 AND is_active THEN profile_name END) AS day_3,
  COUNT(DISTINCT CASE WHEN days_since_signup = 7 AND is_active THEN profile_name END) AS day_7,
  COUNT(DISTINCT CASE WHEN days_since_signup = 14 AND is_active THEN profile_name END) AS day_14,
  COUNT(DISTINCT CASE WHEN days_since_signup = 30 AND is_active THEN profile_name END) AS day_30

FROM `streaming_engagement.user_activity_with_cohort`
GROUP BY cohort_month
ORDER BY cohort_month;
