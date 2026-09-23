# Big Data Analytics – NYC Yellow Taxi Trip Analysis

## Project Overview

This project presents an end-to-end Big Data Analytics pipeline for analysing New York City Yellow Taxi trip data. The project demonstrates how distributed technologies can be used to store, process, clean and analyse a large-scale dataset.

The pipeline uses **Apache Hadoop HDFS** for distributed storage, **YARN** for resource management, **Hive and MapReduce** for distributed query processing, and **Apache Spark** for faster analytics and machine learning.

The analysis focuses on taxi demand patterns, trip characteristics, congestion-related indicators, payment behaviour, trip-duration prediction and spatial demand zones.

---

## Problem Statement

New York City's taxi demand changes significantly depending on the time of day, day of the week and location. This creates operational challenges for drivers, fleet operators and city planners.

The objective of this project is to analyse NYC Yellow Taxi trip records and identify useful patterns that can support:

* Understanding taxi demand by hour and day
* Identifying high-demand pickup locations
* Analysing trip speed as a proxy for congestion
* Understanding payment and tip patterns
* Predicting taxi trip duration
* Identifying demand zones using clustering
* Demonstrating how Big Data technologies can process large datasets

The project uses more than one million taxi trip records from January 2015 in the analytical extract. The report records **1,048,575 trips and 19 variables** in the supplied extract.

---

## Why Big Data Technologies?

The dataset contains a large number of records, making distributed processing useful for demonstrating how data can be stored and processed across a Hadoop ecosystem.

The project compares traditional single-machine processing with a distributed approach using Hadoop.

The main Big Data characteristics considered are:

* **Volume** – more than one million taxi trips in the analytical extract
* **Velocity** – taxi trips are continuously generated and the source data is published periodically
* **Variety** – the dataset contains timestamps, GPS coordinates, categorical variables and monetary values
* **Veracity** – data validation rules were applied to identify invalid or unrealistic records
* **Value** – the analysis produces operational insights about demand, congestion and trip duration

The report identifies **30,031 records (2.86%)** that failed at least one of the defined validity rules before the cleaned dataset was used for analysis.

---

## Dataset

The project uses the **NYC Yellow Taxi Trip Data** for January 2015.

### Dataset information

| Attribute                | Description                       |
| ------------------------ | --------------------------------- |
| Dataset                  | NYC Yellow Taxi Trip Data         |
| Period                   | January 2015                      |
| Analytical records       | 1,048,575                         |
| Variables                | 19                                |
| Analytical extract size  | Approximately 147.7 MB            |
| Main source              | NYC Taxi and Limousine Commission |
| Dataset accessed through | Kaggle                            |

Important variables include:

* Pickup and drop-off timestamps
* Pickup and drop-off coordinates
* Trip distance
* Passenger count
* Rate code
* Payment type
* Fare amount
* Tip amount
* Tolls
* Total amount

The project also uses the larger monthly CSV for demonstrating HDFS storage and block distribution.

---

# Technology Stack

The main technologies used in this project are:

* **Apache Hadoop 3.3.6**
* **HDFS**
* **YARN**
* **Apache Hive 3.1.3**
* **MapReduce**
* **Apache Spark 3.5.3**
* **PySpark**
* **Spark MLlib**
* **Python**
* **HiveQL**
* **Jupyter/Google Colab**
* **Matplotlib / visualisation tools**

---

# Project Architecture

The overall workflow follows this pipeline:

```text
NYC Yellow Taxi Dataset
          │
          ▼
   Data Conversion
   Excel → CSV
          │
          ▼
       HDFS
 Distributed Storage
          │
          ▼
       YARN
 Resource Management
          │
          ├───────────────┐
          ▼               ▼
       Hive           MapReduce
   Data Cleaning       Processing
          │               │
          └───────┬───────┘
                  ▼
             Clean Data
                  │
                  ▼
              Apache Spark
                  │
        ┌─────────┼──────────┐
        ▼         ▼          ▼
   Descriptive    ML      Clustering
    Analytics   Models    Demand Zones
        │         │          │
        └─────────┼──────────┘
                  ▼
           Business Insights
```

---

# 1. Hadoop Environment

A pseudo-distributed Hadoop cluster was configured for the project.

The environment contains:

* **NameNode** – manages HDFS metadata and the namespace
* **DataNode** – stores HDFS data blocks
* **Secondary NameNode** – performs checkpointing
* **ResourceManager** – manages YARN resources
* **NodeManager** – runs containers and processing tasks

The Hadoop configuration used a **128 MB HDFS block size**.

The project also demonstrates the role of:

* FSImage
* EditLog
* HDFS checkpointing
* Data block distribution
* Replication

---

# 2. Data Ingestion into HDFS

The original dataset was converted from Excel to CSV using a streaming Python script.

The data was then uploaded to HDFS using Hadoop command-line tools.

Example commands used in the project include:

```bash
hdfs dfs -mkdir -p /user/hadoop/nyctaxi/raw
hdfs dfs -mkdir -p /user/hadoop/nyctaxi/extract

hdfs dfs -put yellow_tripdata_2015-01.csv /user/hadoop/nyctaxi/raw/

hdfs dfs -put yellow_tripdata_2015-01_extract.csv /user/hadoop/nyctaxi/extract/
```

HDFS commands were also used to inspect files, blocks and their locations.

The project demonstrates HDFS replication using:

```bash
hdfs dfs -setrep 3 /user/hadoop/nyctaxi/raw/yellow_tripdata_2015-01.csv
```

Because the environment uses a single DataNode, increasing replication to three results in under-replicated blocks.

---

# 3. Data Cleaning with Hive

A Hive external table was created over the raw CSV data using a schema-on-read approach.

The data was then cleaned using validation rules.

The main validation rules included:

* Pickup coordinates must fall within the defined NYC bounding box
* Trip duration must be between 1 and 180 minutes
* Trip distance must be greater than 0 and no more than 100 miles
* Fare amount must be between $2.50 and $500
* Passenger count must be between 1 and 6

After applying the validation rules, **1,018,544 trips remained**, representing approximately **97.14%** of the analytical extract.

The cleaned data was written into a columnar ORC table for further analysis.

---

# 4. MapReduce Processing

Hive queries were executed using the MapReduce engine.

One of the main analyses calculated taxi demand by pickup hour, together with average fare and average speed.

The MapReduce workflow can be summarised as:

```text
Input Data
    ↓
Mapper
    ↓
Combiner
    ↓
Shuffle and Sort
    ↓
Reducer
    ↓
Final Results
```

The mapper processes input splits and produces intermediate key-value pairs.

The shuffle stage groups records by key, while reducers combine the partial results to produce the final output.

The project also includes an explicit Hadoop Streaming implementation using:

* `mapper.py`
* `reducer.py`

---

# 5. YARN Job Execution

YARN was used as the resource-management layer.

The general execution process was:

1. Hive or Spark submits an application to the ResourceManager.
2. YARN starts an ApplicationMaster.
3. The ApplicationMaster requests containers.
4. NodeManagers launch processing tasks.
5. Tasks process the data.
6. Results are written back to HDFS.
7. Resources are released after completion.

This demonstrates how storage and resource management are separated within the Hadoop ecosystem.

---

# 6. Apache Spark Analytics

Apache Spark was used for further data processing and analytics.

The Spark workflow uses DataFrame operations and includes transformations such as:

* Filtering
* Column transformations
* Grouping
* Aggregation

Spark transformations are lazy, meaning that they build a logical execution plan before an action triggers execution.

The project also examines the Spark physical execution plan and compares Spark processing with Hive on MapReduce.

---

# 7. Hive vs Spark

The project compares Hive running on MapReduce with Spark.

### Hive / MapReduce

* Suitable for batch-oriented processing
* Uses MapReduce jobs
* Writes intermediate results to disk
* Uses SQL through HiveQL
* Suitable for large-scale batch ETL

### Spark

* Uses in-memory processing
* Supports pipelined execution
* Can reuse executors
* Supports Python APIs
* Suitable for iterative analytics and machine learning

On the project environment, Spark analytical queries using cached cleaned data completed in approximately one to two seconds.

---

# 8. Descriptive Analytics

The project analyses taxi demand patterns across different time periods.

Some of the main findings include:

* Demand was lowest around **04:00**
* Demand was highest around **19:00**
* The highest hourly demand was approximately **7.3 times** the lowest
* Average trip distance was approximately **2.79 miles**
* Average trip cost was approximately **$14.72**
* Average speed was used as a proxy for congestion
* Credit cards represented approximately **62% of trips**
* Credit-card trips generated approximately **68.5% of revenue**

The analysis also examined demand by weekday and the impact of unusual weather conditions during January 2015.

---

# 9. Machine Learning – Trip Duration Prediction

A machine learning model was developed using **Spark MLlib** to predict taxi trip duration.

The model used information available at the beginning of a trip, such as:

* Trip distance
* Pickup hour
* Weekday/weekend indicator
* Rush-hour indicator
* Passenger count
* Vendor
* Pickup coordinates
* Drop-off coordinates
* Rate code

Fare, tip and toll variables were excluded from the model to reduce the risk of target leakage.

Three approaches were compared:

| Model             |     RMSE |      MAE |    R² |
| ----------------- | -------: | -------: | ----: |
| Mean baseline     | 9.04 min | 6.48 min | 0.000 |
| Linear Regression | 5.32 min | 3.62 min | 0.655 |
| Random Forest     | 4.44 min | 2.85 min | 0.759 |

The Random Forest model achieved an **R² of 0.759** and an MAE of approximately **2.85 minutes** on the held-out test set.

---

# 10. Demand Zone Clustering

K-Means clustering was applied to pickup coordinates to identify taxi demand zones.

The project used **8 clusters** for operational interpretation.

The analysis identified major pickup concentrations around:

* Midtown
* Chelsea / Flatiron
* Lower Manhattan
* Upper East Side
* LaGuardia Airport
* JFK Airport

The largest pickup zone was Midtown, accounting for approximately **53.1%** in the project's analysis.

---

# 11. Business Insights

The analysis provides several operational insights.

### Demand-based scheduling

Taxi availability can be aligned with periods of higher demand, particularly evening hours.

### Location-based positioning

High-demand pickup zones can be used to position idle vehicles more effectively.

### ETA prediction

The trip-duration model can support estimated arrival or journey-duration ranges.

### Weather-aware planning

The January snowstorm demonstrates how unusual weather can significantly affect taxi demand.

### Congestion analysis

Average trip speed provides a simple indicator of congestion patterns throughout the day.

---

# 12. Project Limitations

The project has several limitations:

* The analytical dataset represents one month of taxi activity.
* Seasonal variation is therefore not captured.
* Live traffic information was not available.
* Weather information was not directly included as a model input.
* The Hadoop environment was pseudo-distributed on a single host.
* Single-node timings do not represent the performance of a real multi-node cluster.
* The project does not represent real-time production processing.

These limitations should be considered before applying the results to a production environment.

---

# 13. Repository Structure

```text
big-data-analytics-nyc-taxi/
│
├── README.md
│
├── notebooks/
│   └── NYC_Taxi_Analytics.ipynb
│
├── scripts/
│   ├── 00_convert_excel_to_csv.py
│   ├── mapper.py
│   ├── reducer.py
│   └── 04_spark_analytics.py
│
├── hive/
│   └── 03_hive_mapreduce.hql
│
├── results/
│   └── figures/
│
└── report/
    └── Big_Data_Analytics_Report.pdf
```

> The exact folder and file names should be changed to match the files actually uploaded to this repository.

---

# 14. How to Run

## Hadoop

Start the Hadoop services:

```bash
start-dfs.sh
start-yarn.sh
```

Check running Hadoop processes:

```bash
jps
```

Expected services include:

```text
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
```

## HDFS

Create the project directories:

```bash
hdfs dfs -mkdir -p /user/hadoop/nyctaxi/raw
hdfs dfs -mkdir -p /user/hadoop/nyctaxi/extract
```

Upload the dataset:

```bash
hdfs dfs -put yellow_tripdata_2015-01.csv /user/hadoop/nyctaxi/raw/
```

## Hive

Run the Hive script:

```bash
hive -f 03_hive_mapreduce.hql
```

## Spark

Run the Spark analytics script:

```bash
spark-submit 04_spark_analytics.py
```

> The commands above assume that Hadoop, Hive and Spark are correctly configured on the local environment.

---

# 15. Key Outcomes

The project demonstrates an end-to-end Big Data workflow:

```text
Data Source
    ↓
Data Ingestion
    ↓
HDFS Storage
    ↓
Data Cleaning
    ↓
Hive / MapReduce
    ↓
Spark Processing
    ↓
Descriptive Analytics
    ↓
Machine Learning
    ↓
K-Means Clustering
    ↓
Business Insights
```

The final analysis combines distributed data processing with machine learning to investigate NYC taxi demand, trip duration and spatial demand patterns.

---

## Academic Project

**Programme:** MSc Big Data Analytics
**Year:** 2026
**Project:** Advanced Big Data Processing using HDFS, YARN, MapReduce Abstraction, and Spark/Hive

---

## References

The main data source and technical references used in this project are documented in the accompanying academic report.

* NYC Taxi and Limousine Commission – TLC Trip Record Data
* Apache Hadoop Documentation
* Kaggle – NYC Yellow Taxi Trip Data
* Academic research papers referenced in the project report

