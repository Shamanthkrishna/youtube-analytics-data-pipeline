# YouTube Analytics Data Pipeline

## Overview
An end-to-end data engineering project that ingests YouTube analytics data using public APIs, processes it using PySpark on Databricks, and stores analytics-ready datasets using a Lakehouse architecture on AWS S3.

## Tech Stack
- Python
- PySpark
- Databricks
- AWS S3
- Delta Lake

## Architecture
![Architecture](docs/architecture.png)

## Data Flow
1. Extract data from YouTube Data API
2. Store raw data in S3 (Bronze layer)
3. Clean and transform data using PySpark (Silver layer)
4. Create analytics-ready tables (Gold layer)
5. Schedule pipelines using Databricks Jobs

## Data Layers
- **Bronze:** Raw API JSON data
- **Silver:** Cleaned and structured data
- **Gold:** Aggregated analytics tables

## Key Features
- Incremental data ingestion
- Schema validation
- Data quality checks
- Partitioned storage
- Job scheduling

## How to Run
Details will be added after implementation.

## Future Enhancements
- Add monitoring and alerts
- Add dashboard layer
