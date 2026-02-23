# STEDI Human Balance Analytics — AWS Lakehouse

How do we build a trustworthy ML-ready dataset from IoT sensor data when customer consent records are incomplete and serial number data is corrupted?

This project builds a multi-zone data lakehouse on AWS to process sensor data from the STEDI Step Trainer, a balance-monitoring IoT device. Raw data from three sources (customer records, accelerometer readings, and step trainer IoT streams) is ingested into a landing zone, sanitized for research consent and data quality issues, and refined into a curated dataset ready for machine learning. All transformations are implemented as AWS Glue jobs written in PySpark.

> This project was completed as part of the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027). The data can be found in this [repository](https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project). Udacity provided the project requirements and source datasets; all SQL table definitions, Glue job scripts, and pipeline logic were implemented independently.

## Data Flow

```
S3 Landing Zone → AWS Glue (PySpark) → S3 Trusted Zone → AWS Glue (PySpark) → S3 Curated Zone
```

Each zone applies progressively stricter filtering:
- **Landing** — raw ingested data, no filtering
- **Trusted** — records from customers who consented to share data for research
- **Curated** — records from consented customers who also have matching accelerometer data, ready for ML

## Record Counts by Zone

| Table | Zone | Records | Notes |
|---|---|---|---|
| customer_landing | Landing | 956 | All customers from fulfillment website |
| accelerometer_landing | Landing | 81,273 | All accelerometer readings from mobile app |
| step_trainer_landing | Landing | 28,680 | All Step Trainer IoT readings |
| customer_trusted | Trusted | 482 | Customers who agreed to share research data |
| accelerometer_trusted | Trusted | 40,981 | Readings from consented customers only |
| step_trainer_trusted | Trusted | 14,460 | Step Trainer readings for consented customers |
| customers_curated | Curated | 482 | Consented customers with accelerometer data |
| machine_learning_curated | Curated | 43,681 | Joined Step Trainer + accelerometer readings by timestamp |

> The consent filter reduced the customer base from 956 → 482 (roughly 50%), which cascaded through all downstream tables. The machine_learning_curated table is larger than step_trainer_trusted because it is an aggregated join of step trainer readings with their matching accelerometer readings at the same timestamp.

## Glue Jobs

**`customer_landing_to_trusted.py`**
Filters the customer landing table to only retain records where `shareWithResearchAsOfDate` is not null, i.e. customers who have actively consented to share their data for research purposes. Writes to `customer_trusted`.

**`accelerometer_landing_to_trusted.py`**
Joins accelerometer landing data against `customer_trusted` on email address to filter out readings from non-consenting customers. Writes to `accelerometer_trusted`.

**`customer_curated.py`**
Further filters `customer_trusted` to only retain customers who have at least one accelerometer reading, ensuring the curated customer base has both consent and sensor data. Writes to `customers_curated`.

**`step_trainer_landing_to_trusted.py`**
Joins step trainer IoT readings against `customers_curated` on serial number to resolve the serial number data quality issue, matching only records where the serial number corresponds to a known curated customer. Writes to `step_trainer_trusted`.

**`machine_learning_curated.py`**
Aggregates `step_trainer_trusted` and `accelerometer_trusted` by joining on customer and timestamp to create a single ML-ready table where each row contains both the step trainer reading and its corresponding accelerometer reading. Writes to `machine_learning_curated`.

## Data Quality Issue

The fulfillment website had a defect where only 30 unique serial numbers were cycled across all customer records, making it impossible to match Step Trainer IoT data back to the correct customer using the landing zone data alone. This was resolved by using the `customers_curated` table, which was built from verified accelerometer data — as the reference for serial number matching rather than the raw landing zone customer records.

## Athena Verification Screenshots

Query results for each zone are included in the `screenshots/` directory:

| Screenshot | Description |
|---|---|
| `customer_landing` | Raw customer records including non-consenting customers |
| `accelerometer_landing` | Raw accelerometer readings |
| `step_trainer_landing` | Raw Step Trainer IoT readings |
| `customer_trusted` | Filtered to consented customers only (482 records) |

## Tools & Technologies

AWS Glue · AWS S3 · AWS Athena · PySpark · Python · SQL
