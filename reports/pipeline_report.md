# Pipeline Report

## Run stats

| source | events_generated | events_enriched | events_filtered | filtered_pct | windows | flagged_count | flagged_rate | max_window_fraud_rate | total_gross_value | total_net_value | discount_value | pipeline_duration_seconds | throughput_events_per_second | status_counts                                    | window_duration | watermark_delay | fraud_threshold | fraud_alert_rate | min_event_time      | max_event_time      |
| ------ | ---------------- | --------------- | --------------- | ------------ | ------- | ------------- | ------------ | --------------------- | ----------------- | --------------- | -------------- | ------------------------- | ---------------------------- | ------------------------------------------------ | --------------- | --------------- | --------------- | ---------------- | ------------------- | ------------------- |
| file   | 5000             | 4394            | 606             | 0.1212       | 29      | 102           | 0.0232       | 0.0614                | 520,395.83        | 492,213.83      | 28,182.00      | 41.53                     | 105.81                       | {'SUCCESS': 4394, 'FAILED': 403, 'PENDING': 203} | 5 minutes       | 30 minutes      | 0.6500          | 0.0800           | 2026-01-15T09:00:05 | 2026-01-15T11:59:57 |

## Input schema

| field             | type      | nullable |
| ----------------- | --------- | -------- |
| transaction_id    | string    | False    |
| user_id           | string    | False    |
| amount            | double    | False    |
| currency          | string    | True     |
| status            | string    | True     |
| merchant_category | string    | True     |
| channel           | string    | True     |
| country           | string    | True     |
| event_time        | timestamp | False    |
| is_fraud          | double    | True     |

## Transform rules

| rule          | detail                                                                        |
| ------------- | ----------------------------------------------------------------------------- |
| status filter | keep SUCCESS transactions only                                                |
| discount      | net_amount = amount * (1 - category discount_rate)                            |
| fraud score   | bounded 0..1 score from amount, category, channel, country, and latent signal |
| window        | 5 minutes event-time windows with 30 minutes watermark                        |
| trigger       | availableNow=True; every query awaits termination                             |

## Sample enriched events

| transaction_id | event_time          | merchant_category | channel | gross_amount | discount_rate | net_amount | fraud_score | is_flagged |
| -------------- | ------------------- | ----------------- | ------- | ------------ | ------------- | ---------- | ----------- | ---------- |
| txn_42_0000000 | 2026-01-15 09:00:05 | grocery           | web     | 100.05       | 0.0300        | 97.05      | 0.2300      | False      |
| txn_42_0000002 | 2026-01-15 09:00:11 | luxury            | mobile  | 356.88       | 0.0400        | 342.60     | 0.2800      | False      |
| txn_42_0000004 | 2026-01-15 09:00:13 | travel            | web     | 117.49       | 0.0500        | 111.62     | 0.1750      | False      |
| txn_42_0000003 | 2026-01-15 09:00:13 | pharmacy          | mobile  | 12.45        | 0.0200        | 12.20      | 0.1650      | False      |
| txn_42_0000006 | 2026-01-15 09:00:16 | luxury            | api     | 258.37       | 0.0400        | 248.04     | 0.3450      | False      |
| txn_42_0000005 | 2026-01-15 09:00:16 | grocery           | pos     | 61.76        | 0.0300        | 59.91      | 0.1100      | False      |
| txn_42_0000008 | 2026-01-15 09:00:17 | dining            | web     | 13.68        | 0.1000        | 12.31      | 0.2300      | False      |
| txn_42_0000007 | 2026-01-15 09:00:17 | luxury            | pos     | 79.46        | 0.0400        | 76.28      | 0.2000      | False      |

## Windowed aggregates

| window_start        | window_end          | event_count | gross_value | net_value | flagged_count | fraud_rate |
| ------------------- | ------------------- | ----------- | ----------- | --------- | ------------- | ---------- |
| 2026-01-15 09:00:00 | 2026-01-15 09:05:00 | 114         | 12,262.37   | 11,665.01 | 7             | 0.0614     |
| 2026-01-15 09:05:00 | 2026-01-15 09:10:00 | 114         | 12,582.25   | 11,873.91 | 3             | 0.0263     |
| 2026-01-15 09:10:00 | 2026-01-15 09:15:00 | 116         | 12,354.15   | 11,700.54 | 1             | 0.0086     |
| 2026-01-15 09:15:00 | 2026-01-15 09:20:00 | 128         | 13,230.41   | 12,584.89 | 2             | 0.0156     |
| 2026-01-15 09:20:00 | 2026-01-15 09:25:00 | 129         | 15,813.26   | 15,006.55 | 4             | 0.0310     |
| 2026-01-15 09:25:00 | 2026-01-15 09:30:00 | 140         | 15,377.74   | 14,567.10 | 3             | 0.0214     |
| 2026-01-15 09:30:00 | 2026-01-15 09:35:00 | 126         | 13,113.81   | 12,380.72 | 2             | 0.0159     |
| 2026-01-15 09:35:00 | 2026-01-15 09:40:00 | 116         | 14,601.02   | 13,839.01 | 3             | 0.0259     |
| 2026-01-15 09:40:00 | 2026-01-15 09:45:00 | 122         | 14,883.36   | 14,054.49 | 2             | 0.0164     |
| 2026-01-15 09:45:00 | 2026-01-15 09:50:00 | 115         | 15,579.75   | 14,810.59 | 1             | 0.0087     |
| 2026-01-15 09:50:00 | 2026-01-15 09:55:00 | 132         | 19,896.99   | 18,781.93 | 5             | 0.0379     |
| 2026-01-15 09:55:00 | 2026-01-15 10:00:00 | 135         | 15,347.88   | 14,473.62 | 0             | 0.0000     |
