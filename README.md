# Product SCD Type 2 Data Pipeline

An end-to-end data engineering pipeline for maintaining **product history using Slowly Changing Dimension (SCD) Type 2**.

The pipeline processes daily product CSV files from **Google Cloud Storage**, performs data quality checks using **Amazon Deequ**, prepares the data with **PySpark**, and loads it into **BigQuery** while preserving historical versions of changed products.

The goal of the project is to demonstrate how a product dimension can be maintained when attributes such as **price, category, supplier, status, or name** change over time.

---

## 🏗️ Architecture

```
<img width="1408" height="768" alt="Gemini_Generated_Image_vfpzajvfpzajvfpz" src="https://github.com/user-attachments/assets/4dbcb6ae-ae87-445d-b975-ac93265c803c" />


```

### Pipeline Flow

**GCS → PySpark → Deequ → SCD Type 2 → BigQuery → Archive**

---

## 🎯 What Problem Does This Solve?

Product information doesn't always stay the same.

For example, a product might initially look like:

```text
Product ID: 101
Name: Laptop
Category: Electronics
Price: 50000
Supplier: ABC
Status: Active
```

Later, its price changes:

```text
Price: 55000
```

Instead of simply overwriting the old value, an **SCD Type 2** approach keeps the previous version and creates a new version.

This allows historical questions such as:

> "What was the price of this product last month?"

or

> "When did this product's supplier change?"

---

## 🔄 How SCD Type 2 Works

The pipeline maintains three important columns:

| Column                 | Purpose                                          |
| ---------------------- | ------------------------------------------------ |
| `effective_start_date` | Date from which the record became active         |
| `effective_end_date`   | Date when the record stopped being active        |
| `is_current`           | Indicates whether the record is currently active |

For example:

```text
product_id | price | start_date | end_date   | is_current
-----------|-------|------------|------------|-----------
101        | 50000 | 2025-04-01 | 2025-05-01 | FALSE
101        | 55000 | 2025-05-01 | 3000-01-01 | TRUE
```

When a change is detected, the previous version is expired and the new version is inserted.

The current implementation compares incoming records against the current BigQuery record using `product_id` and product attributes including:

* `name`
* `category`
* `price`
* `supplier`
* `status`

---

## 🧹 Data Quality

Before the data reaches the warehouse, the pipeline runs quality checks using **Amazon Deequ / PyDeequ**.

The current checks verify that:

* The dataset is not empty
* `product_id` is not null
* `product_id` is unique

If the checks fail, the pipeline raises an error and stops processing.

This prevents invalid product data from continuing through the pipeline.

---

## ⚡ PySpark Processing

The main Spark application is responsible for coordinating the pipeline.

The processing flow is:

```text
Read
 ↓
Data Quality Checks
 ↓
SCD2 Preparation
 ↓
Write to BigQuery Staging
 ↓
Merge into Product Dimension
 ↓
Archive Input File
```

The Spark application can also receive a processing date:

```bash
--date 2025_05_01
```

If no date is provided, the application uses the current UTC date.

---

## ☁️ Google Cloud Services

| Service                  | Role                                           |
| ------------------------ | ---------------------------------------------- |
| **Google Cloud Storage** | Stores incoming and archived product CSV files |
| **Dataproc**             | Runs the PySpark application                   |
| **BigQuery**             | Stores staging and product dimension tables    |

---

## 📂 Project Structure


### Main Components

#### `main.py`

Acts as the entry point for the Spark application and controls the overall processing sequence.

#### `etl.py`

Handles:

* Reading daily product data from GCS
* Adding the SCD Type 2 bookkeeping columns

#### `deequ_checks.py`

Contains the data quality checks implemented with PyDeequ.

#### `spark_builder.py`

Creates the SparkSession and configures the Deequ dependency.

#### `writer.py`

Handles:

* Writing data to the BigQuery staging table
* Running the SCD Type 2 merge logic
* Archiving processed CSV files in GCS

---

## 🚀 Running the Pipeline

The project includes a Spark submission command in:

```text
spark-submit.txt
```

The application can be submitted with a processing date:

```bash
spark-submit ... main.py --date 2025_05_01
```

The Spark job is configured for cluster deployment and includes the required Deequ and PyDeequ dependencies.

---

## 📊 BigQuery Data Flow

The data passes through a staging table before reaching the final product dimension.

```text
Daily CSV
   │
   ▼
PySpark
   │
   ▼
dim_products_staging
   │
   ▼
SCD Type 2 MERGE
   │
   ▼
dim_products
```

The staging table contains the incoming product records along with the SCD Type 2 fields.

---

## 📦 Data Archiving

After successful processing, the input file is moved from the input location to an archive location.

```text
GCS

products/input/
       │
       │ Processing complete
       ▼
products/archive/
```

This helps keep processed files separate from files waiting to be processed.

---

## 🛠️ Technologies Used

* **Python**
* **PySpark**
* **Apache Spark**
* **Dataproc**
* **Google Cloud Storage**
* **BigQuery**
* **PyDeequ / Amazon Deequ**
* **GitHub**

---

## 💡 Key Concepts Demonstrated

This project focuses on practical data engineering concepts including:

* Slowly Changing Dimensions — Type 2
* Incremental/daily data processing
* Data quality validation
* PySpark-based ETL
* BigQuery staging and merge operations
* Historical data tracking
* Cloud storage management
* Processed-file archiving
* Distributed Spark execution

---

## 📌 Project Goal

The main goal of this project was to understand how a **real-world product dimension can maintain historical changes instead of simply overwriting existing data**.

It also provided hands-on experience with building a Spark-based ETL process, adding data quality validation, and integrating the processing workflow with Google Cloud Storage and BigQuery.
