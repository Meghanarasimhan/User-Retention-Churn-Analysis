#  User-Retention-Churn-Analysis

A BigQuery-based mini-project for analyzing user retention, engagement, and churn using raw session activity and SQL transformations. Organized by topic and built for reproducibility.

---

##  Repository Structure


- **`01_daily_activity_snapshot.sql`** – Builds daily user-level session summaries.
- **`02_cumulative_fact_table.sql`** – Creates cumulative metrics over time per user.
- **`03_User_status_snapshot.sql`** – Captures user status attributes such as active/churned and total active days.
- **`04_user_churn_final_snap_shot.sql`** – Determines final churn status per user using snapshot logic.
- **`05_temporal_trend_analysis.sql`** – Analyzes time-based trends in engagement metrics.
- **`06_user_cumulative_state.sql`** – Shows evolving user state across time windows.
- **`07_scaffold_user_date.sql`** – Generates calendar scaffold for filling gaps.
- **`08_cohort_retention_matrix.sql`** – Builds a cohort-based retention matrix.
- **`09_calendar_dim.sql`**, **`10_devices_dim.sql`** – Dimension tables for date and device enrichments.
- **CSV files** – Base data for profiles, search history, and viewing activity.

---

## Tech Stack 

- Google BigQuery
- Google Cloud Console
- Standard SQL

---

## Key Features Explained

Daily snapshots & cumulative aggregates for user activity.
Churn labeling logic based on inactivity thresholds.
Temporal trend analysis to observe user behavior over time.
Cohort retention matrices to measure user stickiness.
Support dimension tables for dates (calendar_dim) and devices (devices_dim).

---

##  Challenges Faced

1. Inconsistent User Status Determination
Problem: Users frequently switched between active and churned status based on short bursts of viewing, making it difficult to define a consistent state.
Solution: Introduced a business rule defining churn as 30+ days of inactivity, and used ROW_NUMBER() logic to isolate the most recent state per user reliably.
2. Missing Dates in User Time Series
Problem: Users with no activity on certain days caused gaps in the daily cumulative tables, which distorted cohort retention and churn tracking.
Solution: Created a user-date scaffold using CROSS JOIN with GENERATE_DATE_ARRAY() to ensure every user had a complete daily timeline, even with 0 activity.

---
