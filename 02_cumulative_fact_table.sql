CREATE OR REPLACE TABLE streaming_engagement.user_engagement_cumulative AS
SELECT
  profile_name,
  activity_date,
  total_sessions,
  total_watch_minutes,

  COUNT(*) OVER (
    PARTITION BY profile_name
    ORDER BY activity_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS active_day_count,

  LAG(activity_date) OVER (
    PARTITION BY profile_name
    ORDER BY activity_date
  ) AS last_active_date,

  DATE_DIFF(
    activity_date,
    LAG(activity_date) OVER (
      PARTITION BY profile_name
      ORDER BY activity_date
    ),
    DAY
  ) AS days_since_last_active,

  CASE
    WHEN LAG(activity_date) OVER (
      PARTITION BY profile_name
      ORDER BY activity_date
    ) IS NULL THEN 'new'
    WHEN DATE_DIFF(
      activity_date,
      LAG(activity_date) OVER (
        PARTITION BY profile_name
        ORDER BY activity_date
      ),
      DAY
    ) = 1 THEN 'returning'
    ELSE 'resurrected'
  END AS engagement_state

FROM streaming_engagement.daily_activity_snapshot
ORDER BY profile_name, activity_date;
