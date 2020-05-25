# Data Modeling with Postgre DB and building ETL Pipeline for Sparkify 

*** 

## Udacity Data Engineer Project 1
***
### Introduction 

Sparkify is a music streaming app. The analytics team is particutlary interested in understanding the what songs thier users are listening to. Currentlt, they don't have an easy way to query thier data, which resides in a directory od JSON logs and user activity on the app, as weel as a directory with JSON metadata on the songs in thier app. 

### TASK
*** 
In this project we will be creating a Postgres Database and create an ETL pipeline to load the data into the database and help the team to anaylize the data. 

### Database and ETL Pipeline
***
Using the song and dataset, we create a start schema which includes: 
   * One Fact Table(**songplays**)
   * Four Dimenstions **user**, **songs**, **artists**, and **time**