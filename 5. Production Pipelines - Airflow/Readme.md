# Sparkify Data Pipeline - Apache Airflow
This project will introduce you to the core concepts of Apache Airflow. 

## Overview 
This project builds a data pipeline for Sparkify using Apache Airflow that automates and monitors the running of an ETL pipeline.

The pipeline loads song and log data in JSON format from s3 and processes the data into analytics tables on Reshift. A star schema is used to allow the Sparkify team to readily run queries to analyze user activity. Airflow regularly schedules this ETL jobs and runs data quality check to enusre the loads has be done porperly.

## Files
* `udac_example_dag.py` has the list of tasks which will be performed by airflow.

* `create_tables.sql` has the SQL queries used to create all the required tables in Redshift.
* `sql_queries.py` has the SQL queries which will be used by the ETL process.

Few custom operators have been built for this which are placed `plugins/operators` directory, which are: 
* `stage_redshift.py` has `StageToRedshiftOperator` for loading data from stage to Redshift.
* `load_dimension.py` has `LoadDimensionOperator` which loads dimention tables' data 
* `load_fact.py` has `LoadFactOperator`which loads fact table data.
* `data_quality.py` has `DataQualityOperator` which run SQL queries to ensure the data is loaded correctly.