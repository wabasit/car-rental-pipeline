
# 🚗 Car Rental Data Pipeline on AWS

This project implements a scalable big data processing pipeline for a car rental marketplace using **AWS EMR**, **Apache Spark**, **AWS Glue**, **AWS Athena**, **AWS Step Functions**, and **AWS Lambda**.

---

## 🧱 Project Architecture

```
S3 (Raw Data)
   |
   v
EMR (Spark Jobs: ETL)
   |
   v
S3 (Processed Parquet Data)
   |
   v
Glue Crawler -> Glue Data Catalog
   |
   v
Athena (Query Engine)
   |
   v
Lambda (Automated Queries)
   |
   v
Step Functions (Orchestration + SNS Notifications)
```

---

## 📂 Datasets

All raw datasets are stored in **Amazon S3** in CSV format.

- `vehicles.csv` - Vehicle metadata
- `users.csv` - Registered user data
- `locations.csv` - Rental location information
- `rental_transactions.csv` - Transaction history with pickup/drop-off info

---

## 🔥 Spark Jobs on EMR

Two ETL jobs are executed using **Spark on EMR**:

### 1. `user_transaction_analysis.py`
- Computes per-user metrics: total transactions, total spent, rental hours, min/max/avg transaction amounts
- Outputs: `user_metrics`, `daily_metrics`

### 2. `vehicle_location_metrics.py`
- Aggregates KPIs by vehicle type and pickup location
- Outputs: `location_metrics`, `vehicle_type_metrics`

All transformed data is saved back to **S3 in Parquet format**.

---

## 🔍 AWS Glue and Athena

- A **Glue Crawler** (`rental_crawl`) automatically crawls the processed S3 bucket and updates the **Glue Data Catalog**.
- **Athena** queries are used to analyze KPIs such as:
  - Highest revenue-generating location
  - Most rented vehicle type
  - Top-spending users

---

## 🤖 Orchestration with AWS Step Functions

### Step Function Workflow:

1. ✅ Create EMR cluster  
2. ✅ Run Spark Job 1 (user metrics)  
3. ✅ Run Spark Job 2 (location/vehicle metrics)  
4. ✅ Trigger Glue Crawler  
5. ✅ Wait and then trigger Athena query Lambda  
6. ✅ Terminate EMR cluster  
7. ✅ Send SNS notification (Success or Failure)

---

## 🛠️ AWS Lambda: Athena Automation

A Lambda function (`rental_lbd`) is triggered by Step Functions to automatically run predefined Athena SQL queries for analysis.

---

## 📊 Output Tables (in Glue/Athena)

| Table Name             | Description                            |
|------------------------|----------------------------------------|
| `daily_metrics`        | Revenue and transaction count per day  |
| `user_metrics`         | Rental stats per user                  |
| `location_metrics`     | KPIs per pickup location               |
| `vehicle_type_metrics` | KPIs per vehicle type                  |

---

## 🔐 IAM Roles & Permissions

Ensure the following IAM roles exist and are attached correctly:

| Role                  | Purpose                        | Key Permissions                      |
|-----------------------|--------------------------------|---------------------------------------|
| `EMR_DefaultRole`     | EMR cluster service role       | `AmazonElasticMapReduceRole`         |
| `EMR_EC2_DefaultRole` | Role for EMR EC2 instances     | `AmazonElasticMapReduceforEC2Role`   |
| `StepFunctionExecutionRole` | For orchestrating services | `glue:*`, `emr:*`, `lambda:Invoke*`, `sns:Publish` |
| Lambda Execution Role | For Athena automation          | `athena:*`, `s3:GetObject`, `s3:PutObject` |

---

## 📨 Notifications

Step Functions sends an **SNS email notification** at the end of the workflow:
- ✅ `Pipeline Success` if completed
- ❌ `Pipeline Failure` on any failure step

---

## 🏁 How to Run

1. ✅ Upload Spark job scripts to S3:  
   e.g., `s3://your-code-bucket/scripts/user_transaction_analysis.py`

2. ✅ Set up raw datasets in `s3://your-raw-data-bucket/raw_data/`

3. ✅ Deploy the Glue crawler and Lambda function

4. ✅ Launch Step Function with the updated state machine definition

---

## ✅ KPIs Computed

- Total revenue and transactions per location  
- Max/min/avg transaction amounts  
- Unique vehicles per location  
- Revenue per vehicle type  
- User engagement and total rental time  
- Daily revenue trends

---

## 📧 Contact

For issues or improvements, please open an issue or contact [Your Name] at [your.email@example.com].
