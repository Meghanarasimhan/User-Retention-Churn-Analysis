
CREATE OR REPLACE TABLE `streaming_engagement.user_daily_cumulative_state` AS
SELECT
  profile_name,
  activity_date,
  total_sessions,
  total_watch_minutes,
  active_day_count,
  last_active_date,
  days_since_last_active,
  engagement_state,
  engagement_state = 'ACTIVE' AS is_active,
  engagement_state = 'CHURNED' AS is_churned
FROM `streaming_engagement.user_engagement_cumulative`;

SELECT * 
FROM `streaming_engagement.user_daily_cumulative_state`
ORDER BY profile_name, activity_date
LIMIT 100;

