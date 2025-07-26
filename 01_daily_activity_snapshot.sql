CREATE OR REPLACE TABLE streaming_engagement.daily_activity_snapshot AS
SELECT
  `Profile Name` AS profile_name,
  DATE(`Start Time`) AS activity_date,
  COUNT(*) AS total_sessions,
  ROUND(
    SUM(
      EXTRACT(HOUR FROM Duration) * 60 +
      EXTRACT(MINUTE FROM Duration) +
      EXTRACT(SECOND FROM Duration) / 60.0
    ),
    2
  ) AS total_watch_minutes
FROM streaming_engagement.raw_viewing_activity
GROUP BY profile_name, activity_date
ORDER BY activity_date, profile_name;
