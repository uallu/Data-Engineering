#Overview 

The aim of this project is to understand the conpets of Data Warehousing along side working with a Cloud Platform (AWS). 
Spakify is startup company who is setting up a data warehouse to collect song data to which thier users are listening. 
The aim is to help Spakify thier user activities and take business decissions. 

#Data 

Song data is stored at Amazon S3 bucket at `s3://udaycity-dend/log_data` and `s3://udacity-dend/song_data` containing the log and song data. 


#Schema

Following the Dimension and Fact tables: 

__Fact Table:__

* _songplays:_

	columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

__Dimension Tables:__

* _users:_

	`columns: user_id, first_name, last_name, gender, level`
* _songs:_

	`columns: song_id, title, artist_id, year, duration`
* _artists:_

	`columns: artist_id, name, location, latitude, longitude`
* _time:_

	`columns: start_time, hour, day, week, month, year, weekday`

#Using the Code: 

* Update config file `dwh.cfg` with your AWS cluster credentials and IAM role. 
* Run the python script `create_table.py` which will create the tables. 
* Run the python script `etl.py` which will read the files from the bucket and push the data into the tabels. 