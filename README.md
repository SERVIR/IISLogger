<a href="https://www.servirglobal.net//">
    <img src="https://tkms.servirglobal.net/static/training/SERVIR_Logo.png" alt="SERVIR Global"
         title="SERVIR Global" align="right" />
</a>

IISLogger
=========
[![Python: 3.9](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![SERVIR: Global](https://img.shields.io/badge/SERVIR-Global-green)](https://servirglobal.net)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.12104260.svg)](https://doi.org/10.5281/zenodo.12104260)

## Introduction:
The ReadIISLogs script reads IIS log files from a specified folder, filters them based on specified criteria, and loads
the matching records into a database table. The point of this is to later be able to extract stats from the table,
such as number of requests for a particular file for download, see where requests are coming from (via IP
geolocation), etc.

## Details:
The idea for this internal automation script was to give a couple of ways to filter IIS log files for rows you may be
interested in collecting for analysis. It compares all files found within the specified folder against the date of the
latest log row stored in the database table. It will skip any log files older than the max date found in the DB. For
daily log files where the file date matches the last processed record date, it will read the file into a pandas
dataframe and then remove any rows with a timestamp older than the latest log row in the database. For additional
log files with a later date, it appends the file's row contents into the pandas dataframe, which is further
filtered for the rows to keep.

###Parameters:
```
1) -f (required): a string representing a folder to attempt to read log files from.
2) -m (optional): a string to compare to the "cs-method" (e.g. "GET" or "POST") to filter for desired rows, and/or
3) -u (optional): a string to search for within the "cs-uri-stem" values to (further) filter for desired rows.
4) -l (optional): a string to denote the type of logging that this script will write.

usage: ReadIISLogs.py [-h]
                        -f IIS_LOG_FOLDER
                        [-m METHOD_TO_FILTER]
                        [-u URI_TO_FILTER]
                        [-l {DEBUG,INFO,WARNING,ERROR}  Deaults to INFO]
```
For instance, if -m = "GET" and -u = "TrainingMaterials/SAR" were passed in, all log file rows (later than the
latest date and time stored in the DB) would first be filtered down to the cs-method = "GET" rows, and then that
set would further be filtered for only the rows where cs-uri-stem contains "TrainingMaterials/SAR". If one
parameter is passed in and the other is not passed in, the log file rows would only be filtered according to the
single parameter passed in. If neither parameter is passed, all log file rows (later than the date and time
stored in the DB) will be gathered.

## Usage Best Practice:
Depending on the amount of log data and the info that you are trying to capture, it might be best to create a specific
table for each set of filter criteria that you are interested in. For instance, we are interested in information
about how many requests have been made for our SAR Handbook training materials since 2019, so we created a table
called "SAR" dedicated to log data that includes "GET" cs-method requests where the cs-uri-stem contains values
containing "TrainingMaterials/SAR".

## Environment:
ReadIISLogs.py has been developed with python 3.9, and makes use of compatible psycopg2, pandas, sqlalchemy, and pickle
libraries, in addition to other standard python libs. A PostgreSQL database was used in this project. Psycopg2 is
used for the DB connection when reading the latest log entry date/time. Sclalchemy's create_engine() is used for
the DB connection when calling the pandas dataframe's to_sql() method to write the data to a table. The
config/connection info for the required DB and table is included in the Pickle.py file.

The script currently expects the rows collected within the IIS log files to match the items included in
the header defined below:
```python
header = "log_date log_time s_ip cs_method cs_uri_stem cs_uri_query s_port cs_username c_ip cs_user_agent cs_referer sc_status sc_substatus sc_win32_status time_taken"
```
**The underscore ("_") is used in the column names in the script (vs. the dash "-" used in the log files) to match
the database table columns.  As a best practice, please avoid using dashes in database column names!**

**_Note! - If your IIS instance is configured to collect a different set of column info, the script will need
to be adjusted accordingly._**

A matching DB table will also be needed, defined as:
```sql
CREATE TABLE [YOUR_LOGS_TABLE]
(
    log_date date,
    log_time time without time zone,
    s_ip character varying(15) COLLATE pg_catalog."default",
    cs_method character varying(20) COLLATE pg_catalog."default",
    cs_uri_stem character varying(500) COLLATE pg_catalog."default",
    cs_uri_query character varying(256) COLLATE pg_catalog."default",
    s_port character varying(6) COLLATE pg_catalog."default",
    cs_username character varying(256) COLLATE pg_catalog."default",
    c_ip character varying(15) COLLATE pg_catalog."default",
    cs_user_agent character varying(400) COLLATE pg_catalog."default",
    cs_referer character varying(256) COLLATE pg_catalog."default",
    sc_status integer,
    sc_substatus integer,
    sc_win32_status integer,
    time_taken integer
)
```
And the following indexes should also be created on the table:
```sql
CREATE INDEX c_ip
    ON [YOUR LOGS TABLE] USING btree
    (c_ip COLLATE pg_catalog."default" varchar_ops ASC NULLS LAST)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;

CREATE INDEX logdate_logtime
    ON [YOUR LOGS TABLE] USING btree
    (log_date ASC NULLS LAST, log_time ASC NULLS LAST)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;
```

## Instructions to prep the script for execution:
1. Verify the columns that your IIS instance is collecting match the columns specified above. (If not, adjust script.)
2. Create a database/table with the matching columns (as shown above).
3. Edit Pickle.py and enter your specific paths and credentials.
4. Save and run Pickle.py. This should generate "config.pkl" file in the same folder. (config.pkl is required.)
5. Verify the python path and desired parameters inside ReadIISLogs.bat then run it to execute the python script.

## Geolocation of IPs:
The GeolocateIPs script (called separately) is meant to read the IIS log table data, in that it will read the unique
IP addresses from within the log table, and for any IIS log IPs that are not already in the Geolocation table, it
will use a local copy of a geoip2 database to retrieve the country associated with each of the IPs and store the
IP and country in the Geolocation table. This script can be run any time and as often as necessary to locate and store
unique IPs from the logs table. The geolocation table will need to be joined with the logs table when reporting
geolocation information. For more info on geoip2, please see:
https://dev.maxmind.com/geoip/geolite2-free-geolocation-data

**To use the GeolocateIPs script, you will need to establish your own local copy of a country geoip2 database.**

Your geolocation database table should be defined as:
```sql
CREATE TABLE [YOUR_GEOLOCATION_TABLE]
(
    ip character varying(15) COLLATE pg_catalog."default" NOT NULL,
    country character varying(60) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT geolocation_pkey PRIMARY KEY (ip)
)
```
And the following index created on the table:
```sql
CREATE INDEX ip
    ON [YOUR_GEOLOCATION_TABLE] USING btree
    (ip COLLATE pg_catalog."default" varchar_ops ASC NULLS LAST)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;```
```

## License and Distribution
This script is distributed by SERVIR under the terms of the MIT License. See
[License](./License) in this directory for more information.

## Privacy & Terms of Use
ClimateSERV abides to all of SERVIR's privacy and terms of use as described
at [https://servirglobal.net/Privacy-Terms-of-Use](https://servirglobal.net/Privacy-Terms-of-Use).
