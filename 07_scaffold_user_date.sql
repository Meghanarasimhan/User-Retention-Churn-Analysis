WITH full_base AS (
  -- This is your scaffold joined with activity info
  WITH all_dates AS (
  SELECT day AS activity_date
  FROM UNNEST(GENERATE_DATE_ARRAY('2021-10-30', '2023-06-20', INTERVAL 1 DAY)) AS day
),
all_users AS (
  SELECT DISTINCT profile_name
  FROM `your_project.streaming_engagement.user_engagement_cumulative`
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
  FROM `your_project.streaming_engagement.user_engagement_cumulative`
)
  SELECT
    s.profile_name,
    s.activity_date,
    a.total_sessions,
    a.total_watch_minutes,
    a.last_active_date,
    a.activity_date IS NOT NULL AS was_active
  FROM user_date_scaffold s
  LEFT JOIN `your_project.streaming_engagement.user_engagement_cumulative` a
    ON s.profile_name = a.profile_name AND s.activity_date = a.activity_date
),

carry_forward AS (
  SELECT *,
    MAX(CASE WHEN was_active THEN activity_date END) OVER (
      PARTITION BY profile_name
      ORDER BY activity_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS carried_last_active_date
  FROM full_base
)
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

SELECT *
FROM finalized
ORDER BY profile_name, activity_date;