CREATE OR REPLACE TABLE streaming_engagement.user_churn_status AS
WITH max_date AS (
  SELECT MAX(activity_date) AS last_dataset_date
  FROM streaming_engagement.daily_activity_snapshot
),
user_last_seen AS (
  SELECT
    profile_name,
    MAX(activity_date) AS last_active_date
  FROM streaming_engagement.daily_activity_snapshot
  GROUP BY profile_name
)

SELECT
  u.profile_name,
  u.last_active_date,
  DATE_DIFF(m.last_dataset_date, u.last_active_date, DAY) AS days_since_last_active,
  CASE
    WHEN DATE_DIFF(m.last_dataset_date, u.last_active_date, DAY) > 14 THEN 'churned'
    ELSE 'active'
  END AS churn_status
FROM user_last_seen u
CROSS JOIN max_date m
ORDER BY profile_name;


CREATE OR REPLACE TABLE streaming_engagement.final_user_snapshot AS
SELECT 
    a.profile_name,
    a.last_active_date,
    a.current_engagement_state,
    a.total_active_days,
    a.total_watch_time,
    b.days_since_last_active,
    b.churn_status
FROM streaming_engagement.user_status_snapshot a
JOIN streaming_engagement.user_churn_status b
  ON a.profile_name = b.profile_name
ORDER BY a.profile_name;
