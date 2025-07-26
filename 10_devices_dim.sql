
CREATE OR REPLACE TABLE `streaming_engagement.device_dim` AS
SELECT * FROM UNNEST([
  STRUCT(0 AS device_type_code, 'Mobile' AS device_type_name),
  STRUCT(1 AS device_type_code, 'Smart TV'),
  STRUCT(2 AS device_type_code, 'Browser'),
  STRUCT(3 AS device_type_code, 'Tablet'),
  STRUCT(4 AS device_type_code, 'Gaming Console'),
  STRUCT(5 AS device_type_code, 'Other')
]);

CREATE OR REPLACE TABLE `streaming_engagement.device_activity_cleaned` AS
SELECT
  d.`Profile Name`,
  dd.device_type_name
FROM `streaming_engagement.all_devices` d
LEFT JOIN `streaming_engagement.device_dim` dd
  ON CAST(REGEXP_EXTRACT(d.`Device Type`, r"Device Type (\d+)") AS INT64) = dd.device_type_code;

CREATE OR REPLACE TABLE `streaming_engagement.user_device_summary` AS
SELECT
  `Profile Name`,
  MAX(IF(device_type_name = 'Mobile', 1, 0)) AS uses_mobile,
  MAX(IF(device_type_name = 'Smart TV', 1, 0)) AS uses_smart_tv,
  MAX(IF(device_type_name = 'Browser', 1, 0)) AS uses_browser,
  MAX(IF(device_type_name = 'Tablet', 1, 0)) AS uses_tablet,
  MAX(IF(device_type_name = 'Gaming Console', 1, 0)) AS uses_gaming_console,
  MAX(IF(device_type_name = 'Other', 1, 0)) AS uses_other
FROM `streaming_engagement.device_activity_cleaned`
GROUP BY `Profile Name`;

CREATE OR REPLACE TABLE `streaming_engagement.final_user_snapshot_enriched` AS
SELECT
  f.*,
  d.uses_mobile,
  d.uses_smart_tv,
  d.uses_browser,
  d.uses_tablet,
  d.uses_gaming_console,
  d.uses_other
FROM `streaming_engagement.final_user_snapshot` f
LEFT JOIN `streaming_engagement.user_device_summary` d
  ON f.profile_name= d.`Profile Name`;

