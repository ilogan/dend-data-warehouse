# AWS Data Warehouse - S3 to Redshift

The goal of this project is to extract a music streaming service's JSON formatted songplay data from S3 and load it into a Redshift data warehouse, so that their analysts can better investigate how users are interacting with their app.

There are three main steps to accomplish this:

1. Design the schema for the staging, fact, and dimension tables in Redshift.

2. Perform ETL to move the JSON data in S3 into Redshift staging tables.

3. Parse the data out of the staging tables into fact and dimension tables for online analytical processing (OLAP).

The database created follows a star schema and is made up of two staging tables, one fact table, and four dimension tables:

**Staging** - `staging_events`, `staging_songs`

**Fact** - `songplays`

**Dimension** - `users`, `songs`, `artists`, `time`

## Files and S3

### Files

`sql_queries.py` - Contains the SQL queries used to manipulate the data. It is the backbone behind `create_tables.py` and `etl.py`, as those scripts simply run these queries when necessary. The queries are used for several major functions:

1. Dropping existing tables
2. Creating the tables in Redshift with associated schema designs
3. Utilizes Redshift's `COPY` command to move the JSON data from S3 to Redshift
4. Inserts data from the staging tables into the fact and dimension tables

`create_tables.py` - Utilizes the psycopg2 library to perform the `DROP` and `CREATE` table queries. This file should be run to create the Redshift database with empty staging, fact, and dimension tables.

`etl.py` - Utilizes psycopg2 to perform Redshift's `COPY`  from S3 as well as `INSERT INTO` from existing staging tables into fact and dimension tables.

**Additional Files**

`dwh.cfg` - Holds AWS credentials to access S3 and Redshift

### S3 Directories

`/song_data` - Contains songs from the [Million Song Dataset](http://millionsongdataset.com/). Files are JSON formatted.

```json
{
	"num_songs": 1,
	"artist_id": "ARD7TVE1187B99BFB1",
	"artist_latitude": null,
	"artist_longitude": null,
	"artist_location": "California - LA",
	"artist_name": "Casual",
	"song_id": "SOMZWCG12A8C13C480",
	"title": "I Didn't Mean To",
	"duration": 218.93179,
	"year": 0
}
```

`/log_data` - Contains user activity logs. Files are JSON formatted.

```json
{
	"artist":null, 
	"auth":"LoggedIn",
	"firstName":"Walter",
	"gender":"M",
	"itemInSession":0,
	"lastName":"Frye",
	"length":null,
	"level":"free",
	"location":"San Francisco-Oakland-Hayward, CA",
	"method":"GET",
	"page":"Home",
	"registration":1540919166796.0,
	"sessionId":38,"song":null,
	"status":200,
	"ts":1541105830796,
	"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
	"userId":"39"
}
```



## Running the Scripts

###Initial Setup

1. Clone this repository, then install any project dependencies found in `requirements.txt`
2. Create a Redshift cluster and ensure it has read access to S3
3. Fill in `dwh.cfg` with your AWS credentials to connect to the database and interact with Redshift.

```
[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=

[IAM_ROLE]
ARN=''

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
```

Now for the scripts: 

1. Run `create_tables.py` to create the Redshift database.
2. After ensuring the tables are created, run `etl.py` to populate the database with the data in S3 (this will take ~10 minutes).

**Note:** if any changes are made to `etl.py` or the database schema, run`create_tables.py` to recreate the database