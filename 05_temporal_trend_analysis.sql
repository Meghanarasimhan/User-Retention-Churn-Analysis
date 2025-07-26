-- SELECT
--   MIN(activity_date) AS min_date,
--   MAX(activity_date) AS max_date
-- FROM `streaming_engagement.user_engagement_cumulative`;

WITH full_base AS (
  WITH all_dates AS (
  SELECT day AS activity_date
  FROM UNNEST(GENERATE_DATE_ARRAY('2021-10-30', '2023-06-20', INTERVAL 1 DAY)) AS day
),
all_users AS (
  SELECT DISTINCT profile_name
  FROM `streaming_engagement.user_engagement_cumulative`
),
user_date_scaffold AS (
  SELECT
    u.profile_name,
    d.activity_date
  FROM all_users u
  CROSS JOIN all_dates d
),
activity_data AS (
  SELECT
    profile_name,
    activity_date,
    total_sessions,
    total_watch_minutes,
    last_active_date
  FROM `streaming_engagement.user_engagement_cumulative`
)

SELECT
  s.profile_name,
  s.activity_date,
  a.total_sessions,
  a.total_watch_minutes,
  a.last_active_date,
  a.activity_date IS NOT NULL AS was_active 
FROM user_date_scaffold s
LEFT JOIN activity_data a
  ON s.profile_name = a.profile_name AND s.activity_date = a.activity_date
ORDER BY s.profile_name, s.activity_date
),

carry_forward AS (
  SELECT *,
    MAX(CASE WHEN was_active THEN activity_date END) OVER (
      PARTITION BY profile_name
      ORDER BY activity_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS carried_last_active_date
  FROM full_base
),
finalized AS (
  SELECT *,
    DATE_DIFF(activity_date, carried_last_active_date, DAY) AS days_since_last_active,
    
    CASE
      WHEN carried_last_active_date IS NULL THEN 'NEW'
      WHEN DATE_DIFF(activity_date, carried_last_active_date, DAY) = 0 THEN 'RETURNING'
      WHEN DATE_DIFF(activity_date, carried_last_active_date, DAY) <= 7 THEN 'RETURNING'
      WHEN DATE_DIFF(activity_date, carried_last_active_date, DAY) <= 30 THEN 'RESURRECTED'
      ELSE 'CHURNED'
    END AS engagement_state,

    DATE_DIFF(activity_date, carried_last_active_date, DAY) <= 7 AS is_active,
    DATE_DIFF(activity_date, carried_last_active_date, DAY) >= 30 AS is_churned
  FROM carry_forward
)
SELECT
  activity_date,
  COUNT(DISTINCT profile_name) AS total_users,
  COUNT(DISTINCT CASE WHEN is_active THEN profile_name END) AS active_users,
  COUNT(DISTINCT CASE WHEN is_churned THEN profile_name END) AS churned_users,
  ROUND(100 * COUNT(DISTINCT CASE WHEN is_active THEN profile_name END) / COUNT(DISTINCT profile_name), 2) AS active_pct,
  ROUND(100 * COUNT(DISTINCT CASE WHEN is_churned THEN profile_name END) / COUNT(DISTINCT profile_name), 2) AS churn_pct
FROM finalized
GROUP BY activity_date
ORDER BY activity_date;

