# Data Lake
The main aim of this project was to build an ETL processes for Sparkify to load song data and user log data from S3 and the processing the data using SPARK and loading into back to S3.


## Design

Sparkify is using STAR schema to build that databases for its analytical purposes.
The fact table(songplays) is surrounded by the dimension tables(song, artist, users, time table)

## Files
 
`etl.py`

This script is repsonsible for collecting the data from S3 bucket, transform the data
as per the design i,e. into FACT and dimension table and load back into S3 bucket.

`dl.cfg`

This config file holds your credentials.

## Execution

- Clone the repo.
- Replace your AWS credentials in `dl.cfg` file.
- Run `python3 etl.py`