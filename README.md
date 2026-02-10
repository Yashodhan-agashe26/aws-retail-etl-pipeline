# aws-retail-etl-pipeline

Serverless Retail ETL Pipeline on AWS

S3 • AWS Glue (PySpark) • Athena • Parquet • SQL

Overview

This project demonstrates an end-to-end serverless ETL data engineering pipeline on AWS.
Raw retail transaction data is ingested into an Amazon S3 data lake, transformed using an AWS Glue PySpark ETL job, stored in optimized Parquet format, and queried through Amazon Athena for analytics and business insights.

The pipeline highlights real-world data engineering challenges such as:

Mixed date formats

Null and inconsistent values

Schema control during ingestion

Feature engineering for profitability analytics

Efficient columnar storage for fast querying

Architecture

Flow

Kaggle Retail Dataset
        ↓
Amazon S3 (Raw Layer)
        ↓
AWS Glue Crawler → Data Catalog
        ↓
AWS Glue PySpark ETL Job
        ↓
Amazon S3 (Processed Parquet Layer)
        ↓
Amazon Athena → SQL Analytics


Key Design Choices

Schema-on-read using Glue Data Catalog

String-first ingestion to prevent parsing failures from dirty data

Parquet + compression to reduce scan cost and improve performance

Serverless architecture (no infrastructure management)

Tech Stack

AWS S3 – Data lake storage

AWS Glue Crawler – Schema discovery

AWS Glue (PySpark) – Data transformation & feature engineering

Amazon Athena – Serverless SQL analytics

Parquet – Optimized columnar storage

SQL & PySpark – Data processing

ETL Pipeline Steps
1. Data Ingestion

Downloaded Superstore retail dataset from Kaggle

Uploaded CSV to S3 raw layer

2. Metadata & Schema Discovery

Created Glue database and crawler

Automatically generated table schema

Validated raw data using Athena

3. Transformation (Core Engineering)

Implemented PySpark ETL job to:

Normalize mixed date formats

Clean null and inconsistent values

Apply safe schema casting

Engineer business metrics:

cost_estimate = sales − profit

discount_amount = sales × discount

Write compressed Parquet to processed S3 layer

4. Analytics Layer

Created Athena external table on Parquet data

Generated insights:

Total revenue

Monthly sales trends

Top profitable products

Sample SQL Queries

Total Revenue

SELECT SUM(sales) AS total_revenue
FROM retail_orders_processed;


Top 10 Profitable Products

SELECT product_name, SUM(profit) AS total_profit
FROM retail_orders_processed
GROUP BY product_name
ORDER BY total_profit DESC
LIMIT 10;


Monthly Sales Trend

SELECT date_trunc('month', order_date) AS month,
       SUM(sales) AS revenue
FROM retail_orders_processed
GROUP BY month
ORDER BY month;

Key Outcomes

Processed ~10K retail records end-to-end

Reduced Athena query scan size significantly using Parquet

Demonstrated production-style defensive ETL in PySpark

Built a fully serverless analytics pipeline on AWS

Learning Highlights

Real-world dirty data handling

Schema control vs schema inference

Columnar storage optimization

Serverless data engineering architecture

End-to-end ETL lifecycle
