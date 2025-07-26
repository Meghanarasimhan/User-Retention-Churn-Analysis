CREATE OR REPLACE TABLE streaming_engagement.user_status_snapshot AS
WITH latest_engagement_state AS (
  SELECT
    profile_name,
    activity_date,
    current_engagement_state,
    total_watch_minutes,
    ROW_NUMBER() OVER (PARTITION BY profile_name ORDER BY activity_date DESC) AS rn
  FROM streaming_engagement.user_engagement_cumulative
)
SELECT
  profile_name,
  MAX(activity_date) AS last_active_date,
  -- pick current engagement state from the latest activity
  MAX(IF(rn = 1, current_engagement_state, NULL)) AS current_engagement_state,
  COUNT(*) AS total_active_days,
  ROUND(SUM(total_watch_minutes), 1) AS total_watch_time
FROM latest_engagement_state
GROUP BY profile_name
ORDER BY profile_name;
