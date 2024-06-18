#
# Lance Gilliland - April 26, 2024
# This script processes the IIS logs from the folder passed in and extracts the contained rows.
# It compares all files found within the folder against the date of the latest log row in the database. It will skip
# any log files older than the max date in the DB. For log files where the date matches the last processed date, it
# will read the file into a dataframe and then remove any rows with a timestamp older than latest log row in the
# database. For additional files with a later date, it appends the file's row contents into the pandas dataframe,
# which is further filtered for the rows to keep.
#
# Script parameters:
#  1) -f - (required) a string representing a folder to attempt to read log files from
#  2) -m - (optional) a string to compare to the "cs-method" (e.g. "GET" or "POST") to filter for desired rows, and/or
#  3) -u - (optional) a string to search for within the "cs-uri-stem" values to (further) filter for desired rows.
#  4) -l - (optional) a string to denote the type of logging that this script will write. (defaults to INFO)
#
# For instance, if -m = "GET" and -u = "TrainingMaterials/SAR" were passed in, all read log file rows would first be
# filtered down to the cs-method = "GET" rows, and then that set would further be filtered for only the rows where
# cs-uri-stem contains "TrainingMaterials/SAR".  If one parameter is passed in and the other is not passed in, the
# log file rows would only be filtered according to the single parameter passed in.  If neither parameter is passed,
# all log file rows (later than the date and time stored in the DB) will be gathered.
#
# A pickle file with the database connection info and desired logfile location is required.
#
# In addition to some standard libraries, this script required the install of psycopg2
#
import pandas

import Util  # Local utility functions used across project

import logging
import argparse  # required for processing command line arguments
import os  # required for checking if path/file exists
import pickle  # for reading a pickle file
import datetime  # for working with dates and times

import pandas as pd  # used to read, store and analyze csv file info
import psycopg2  # used for database stuff


# Set up the argparser to capture any arguments...
def setupArgs():
    parser = argparse.ArgumentParser(__file__,
                                     description="This function takes an IIS log file as input and extracts"
                                                 " info into a log file.")

    parser.add_argument("-f", "--iis_log_folder",
                        help="Folder path to IIS Log files to analyze.",
                        required=True, type=str)
    parser.add_argument("-m", "--method_to_filter",
                        help="cs-method to use as a filter for the log entries. e.g. GET or POST",
                        type=str, default="")
    parser.add_argument("-u", "--uri_to_filter",
                        help="string contained within the cs-uri-stem to use as a filter for the log "
                             "entries. e.g. TrainingMaterial/SAR",
                        type=str, default="")
    parser.add_argument("-l", "--logging",
                        help="Logging level to report. Default: INFO",
                        type=str, choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")

    return parser.parse_args()


def readLastDateTimeProcessed(dbname, dbuser, dbpassword, dbhost, logstable):
    # Get Last processed datetime from the data store. Combines the log_date and log_time from the data store.
    # If no error, but simply no datetime found, return an early default datetime!
    # Returns either a datetime object or None if there was an error!
    try:
        theDate = None

        # Create a psycopg2 db connection with the params passed in
        conn = psycopg2.connect(dbname=dbname, user=dbuser, password=dbpassword, host=dbhost)

        # Create a cursor object
        cur = conn.cursor()

        # Read the latest date and related time from the table
        # cur.execute(f"SELECT log_date, log_time FROM {logstable} WHERE log_time IN "
        #             f"(SELECT MAX(log_time) FROM {logstable} WHERE log_date IN "
        #             f"(SELECT MAX(log_date) FROM {logstable}))")
        cur.execute(f"SELECT log_date, MAX(log_time) FROM {logstable} WHERE log_date IN "
                    f"(SELECT MAX(log_date) FROM {logstable}) "
                    f"GROUP BY log_date")
        rows = cur.fetchall()
        for row in rows:
            oLogDate = row[0]
            oLogTime = row[1]
            # Note - The log_date field is already a date object in the db (and log_time is a timestamp field), so
            # those values are already in the date and time object when returned.
            # Build a datetime object with the date and time...
            oLogDateTime = datetime.datetime(oLogDate.year, oLogDate.month, oLogDate.day, oLogTime.hour,
                                             oLogTime.minute, oLogTime.second)
            logging.info('%s - Last log record datetime from DB.' % oLogDateTime)
            print('%s - Last log record datetime from DB.' % oLogDateTime)
            theDate = oLogDateTime
            break

        # Close the cursor and connection
        cur.close()
        conn.close()

        # If we get here and there is no date set, it could be the first time this is run, just return an early default.
        if theDate is None:
            # Return an older default date
            theDate = datetime.datetime.strptime("1972/06/30", "%Y/%m/%d")
            logging.info('NO DATE/TIME FOUND IN DB, returning 1972/06/30')
            print('NO DATE/TIME FOUND IN DB, returning 1972/06/30')

        return theDate

    except psycopg2.Error as e:
        logging.error("### Error connecting to database: ", e)
        print("### Error connecting to database: ", e)
        return None

    except Exception as e:
        logging.error('### Error occurred in readLastDateProcessed() ###, %s' % e)
        print('### Error occurred in readLastDateProcessed() ###, %s' % e)
        return None


def readFileTheHardWay(sfile, numCols):
    # Read the file line by line into a list of lists, skipping any error rows...
    # Specifically using the errors="replace" option to avoid encoding errors when reading/writing the file.
    #     See: https://docs.python.org/3/library/functions.html#open
    # Check that each line read has the desired number of columns before including it.
    # Function returns a pandas dataframe.
    try:
        pdf = pd.DataFrame()  # Initialize the return object
        lines = []  # List of lists - built and converted to a dataframe
        # with open(sfile, 'r', encoding="ascii", errors="surrogateescape") as f:
        with open(sfile, 'r', encoding="ascii", errors="replace") as f:
            linestr = f.readline()
            while linestr:
                # Check the first character to see if it is a comment("#") - if not, process the line
                if linestr[:1] != "#":
                    # Split the linestr to check the number of columns. Note, split() returns a list...
                    lineitems = linestr.split(' ')
                    if len(lineitems) == numCols:
                        # Add lineitems list to the lines list
                        lines.append(lineitems)
                # Move on to the next line
                linestr = f.readline()

        # If we found any lines, convert to dataframe
        if len(lines) > 0:
            pdf = pd.DataFrame(lines)

        return pdf

    except:
        err = Util.capture_exception()
        logging.error(err)
        return pdf


def readNewerLogs(folder, odatetimeLastStored):
    # For any files found in the folder passed in:
    #   Check each log file date (yyyy-mm-dd) against the odatetimeLastStored (yyyy-mm-dd) passed in and:
    #   - if the file "date" is >= odatetimeLastStored "date"...       (note - excludes timestamp)
    #       Read / process the log file to the dataframe, BUT...
    #           If file "date" == odatetimeLastStored "date" only keep new rows where log datetime > odatetimeLastStored
    #   - if file "date" is < odatetimeLastStored "date"...
    #       Do not process the log file. We want to ignore any logs that are < the odatetimeLastStored in the DB.
    try:
        # Initialize the return object
        df_return = pd.DataFrame()

        sdatetimeLastStored = datetime.datetime.strftime(odatetimeLastStored, "%Y-%m-%d %H:%M:%S")
        # header = "log_date log_time s_ip cs_method cs_uri_stem cs_uri_query s_port cs_username c_ip cs_user_agent " \
        #          "cs_referer sc_status sc_substatus sc_win32_status time_taken"
        # NOTE - The old Einstein server did not capture the cs_referer column, so had to adjust...
        header = "log_date log_time s_ip cs_method cs_uri_stem cs_uri_query s_port cs_username c_ip cs_user_agent " \
                 "sc_status sc_substatus sc_win32_status time_taken"
        columns = header.split(" ")
        sep = " "

        logging.info('Reading logs from folder: {0}'.format(folder))
        print('Reading logs from folder: {0}'.format(folder))
        # Grab the file names from within the folder
        files = [os.path.join(folder, item) for item in os.listdir(folder)
                 if os.path.isfile(os.path.join(folder, item))]

        logging.info("Files found: {0}".format(str(len(files))))
        print("Files found: {0}".format(str(len(files))))

        # Build a list of dataframes - one dataframe for each log file found
        frames = []
        for file in files:
            # Extract the date from the filename to compare against the odatetimeLastStored passed in.
            sfName = file.rsplit('\\', 1)[-1]   # u_ex240419.log
            sfDate = sfName[4:10]   # strip out 240419
            oFileDate = datetime.datetime.strptime(sfDate, "%y%m%d")
            if oFileDate.date() >= odatetimeLastStored.date():
                logging.info(f'\tLoading file: {file}')
                print(f'\tLoading file: {file}')
                # Read the file into a dataframe... we will set the columns later, so do not read a header row.

                # Note that some logs were giving the error:
                #       "codec can't decode byte 0x84 in position 116: invalid start byte"
                # Turns out there are some goofy characters in some of the files.
                # So if we get an error on read_csv(), lets try to read the file line by line and skip any errors...
                try:
                    # df_tmp = pd.read_csv(file, sep=sep, comment='#',
                    #                      header=None, encoding_errors='replace')     # This gave different results
                    df_tmp = pd.read_csv(file, sep=sep, comment="#", header=None)
                    df_tmp.columns = columns
                except Exception as e:
                    logging.info('Reading this file the hard way...')
                    print('Reading this file the hard way...')
                    df_tmp = readFileTheHardWay(file, len(columns))
                    df_tmp.columns = columns

                # If the date already captured equals the file date, we need to do more to filter the rows...
                if odatetimeLastStored.date() == oFileDate.date():
                    # Build a new datetime column in df_tmp by combining the log_date and log_time cols...
                    logging.info('\t\tFiltering log file for entries later than: {0}'.format(sdatetimeLastStored))
                    print('\t\tFiltering log file for entries later than: {0}'.format(sdatetimeLastStored))
                    df_tmp['log_datetime'] = (pd.to_datetime(df_tmp['log_date'].astype(str) + " " + df_tmp['log_time']))
                    # Use the new column to keep only rows where the new column is > odatetimeLastStored.
                    df_tmp = df_tmp[(df_tmp['log_datetime'] > sdatetimeLastStored)]
                    # Now that we've removed any "older" rows, we can drop the added log_datetime column.
                    df_tmp = df_tmp.drop(['log_datetime'], axis=1)

                # Truncate some of the values which sometimes run long... to match the table/col definitions
                df_tmp["cs_uri_stem"] = df_tmp["cs_uri_stem"].str[:499]
                df_tmp["cs_uri_query"] = df_tmp["cs_uri_query"].str[:255]
                df_tmp["cs_username"] = df_tmp["cs_username"].str[:255]
                df_tmp["cs_user_agent"] = df_tmp["cs_user_agent"].str[:399]
                # NOTE - The old Einstein server did not capture the cs_referer column, so had to adjust...
                # df_tmp["cs_referer"] = df_tmp["cs_referer"].str[:255]

                # Append the resulting log rows dataframe to the list of dataframes
                frames.append(df_tmp)

            else:
                logging.info(f'\tSKIPPING file: {file}')
                print(f'\tSKIPPING file: {file}')

        # In not empty, add the list of dataframes into the dataframe to be returned.
        if frames:
            df_return = pd.concat(frames)

        return df_return

    except:
        err = Util.capture_exception()
        logging.error(err)
        return df_return


def queryDB(dbname, dbuser, dbpassword, dbhost, logstable, geolocatetable):
    # Query the DB - joining the 'logstable' and the geolocation ip table - and return a dataframe with the results.
    try:
        # Create a psycopg2 db connection with the params passed in
        conn = psycopg2.connect(dbname=dbname, user=dbuser, password=dbpassword, host=dbhost)

        # Create a cursor object
        cur = conn.cursor()

        logging.info('Querying database for country requests...')
        print('Querying database for country requests...')

        # Select the total requests per country
        cur.execute(f"SELECT country, count(*) as requests FROM {geolocatetable} "
                    f"INNER JOIN {logstable} ON {geolocatetable}.ip={logstable}.c_ip "
                    f"GROUP BY country ORDER BY requests DESC")
        df = pd.DataFrame(cur.fetchall(), columns=['Country', 'Requests'])

        # # Select the requests per country - broken down by year and month
        # cur.execute(f"SELECT date_part('year', log_date) AS LogYear, "
        #             f"date_part('month', log_date) AS LogMonth, "
        #             f"country AS Country, "
        #             f"COUNT(*) AS Requests "
        #             f"FROM {geolocatetable} INNER JOIN {logstable} ON {geolocatetable}.ip={logstable}.c_ip "
        #             f"GROUP BY LogYear, LogMonth, Country ORDER BY LogYear, LogMonth, Requests DESC")
        # df = pd.DataFrame(cur.fetchall(), columns=['LogYear', 'LogMonth', 'Country', 'Requests'])

        # Close the cursor and connection
        cur.close()
        conn.close()

        # Return the created dataframe
        return df

    except psycopg2.Error as e:
        logging.error("### Error connecting to database: ", e)
        print("### Error connecting to database: ", e)
        return None

    except Exception as e:
        logging.error('### Error occurred in queryDB() ###, %s' % e)
        print('### Error occurred in queryDB() ###, %s' % e)
        return None


def writeDFtoFile(theDF, reportPath, reportFile):
    # Write the DF to a .csv file
    try:
        if not theDF.empty:
            if os.path.exists(reportPath):
                logging.info('Writing report file...')
                print('Writing report file...')
                theDF.to_csv(reportPath + reportFile, index=False)
            else:
                logging.info('Specified ReportPath does not exist: {0}'.format(reportPath))
                print('Specified ReportPath does not exist: {0}'.format(reportPath))
        else:
            logging.info('Nothing to report.')
            print('Nothing to report.')

    except Exception as e:
        logging.error('### Error writing dataframe to Report file. ###, %s' % e)
        print('### Error writing dataframe to Report file. ###, %s' % e)


# Main entry point...
if __name__ == '__main__':

    # Setup any required and/or optional arguments to be passed in.
    # If required args are not provided, the usage message will be displayed.
    args = setupArgs()

    try:
        # Read the pickle config file to get variables
        pkl_file = open('config.pkl', 'rb')
        myConfig = pickle.load(pkl_file)
        pkl_file.close()

        logDir = myConfig['logFileDir']
        # Get log folder and check if it exists
        if not os.path.exists(logDir):
            raise FileNotFoundError('Specified log folder does not exist: {0}'.format(logDir))
        logging.basicConfig(filename=logDir + '\\ReadIISLogs_' + datetime.date.today().strftime('%Y-%m-%d') + '.log',
                            level=args.logging,
                            format='%(asctime)s: %(levelname)s --- %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p')

        logging.info('------------------------- Processing Starting -------------------------')
        logging.info(r"Logging level set to: {0}.  [Pass in parameter `-l DEBUG` for verbose messaging!"
                     "  Available values are: DEBUG, INFO, WARNING, ERROR]".format(args.logging))

        # Get a start time for the script.
        time_ScriptStarted = Util.get_StartTime()

        dbname = myConfig['dbname']
        dbuser = myConfig['dbuser']
        dbpassword = myConfig['dbpassword']
        dbhost = myConfig['dbhost']
        logstable = myConfig['logstable']
        geolocatetable = myConfig['geolocatetable']

        # Build the connection string for the psycopg2 connection to postgres
        # conn_string = 'postgresql://postgres:@bergson.socrates.work/iis_logs'
        conn_string = 'postgresql://' + dbuser + ':' + dbpassword + '@' + dbhost + '/' + dbname

        # ----------------------------------------------------
        # Retrieve the last data processed from the data store
        # Either a date or None will be returned. If None, there was a problem...
        # ----------------------------------------------------
        lastDateTimeProcessed = readLastDateTimeProcessed(dbname, dbuser, dbpassword, dbhost, logstable)
        if lastDateTimeProcessed is not None:
            # ---------------------------------------------------
            # Read the 'latest' log files into a pandas dataframe
            # ---------------------------------------------------
            df_logs = readNewerLogs(args.iis_log_folder, lastDateTimeProcessed)
            if df_logs.empty:
                logging.info("NO NEW LOG DATA FOUND!")
                print("NO NEW LOG DATA FOUND!")
            else:
                # Start reducing the rows from the logs to just the data we are interested in!
                # Based on the Method and URI string passed in, filter down the log rows...
                logging.info("Reducing dataframe rows based on method and URI parameters...")
                print("Reducing dataframe rows based on method and URI parameters...")
                df_logs_Filtered = pd.DataFrame()  # Init because it may not be used and we check it further below...
                if len(args.method_to_filter) > 0 and len(args.uri_to_filter) > 0:
                    # First apply the Method filter...
                    filter = df_logs["cs_method"] == args.method_to_filter
                    df_logs_Method = df_logs.where(filter).dropna()

                    # Then apply the URI filter to the already filtered Method results...
                    filter = df_logs_Method["cs_uri_stem"].str.contains(args.uri_to_filter)
                    df_logs_Filtered = df_logs_Method.where(filter).dropna()
                elif len(args.method_to_filter) > 0:
                    # If we get here, we know that no URI filter was specified, so just filter
                    # the entire results for the Method...
                    filter = df_logs["cs_method"] == args.method_to_filter
                    df_logs_Filtered = df_logs.where(filter).dropna()
                elif len(args.uri_to_filter) > 0:
                    # If we get here, we know that no Method filter was specified, so just filter
                    # the entire results for the URI string...
                    filter = df_logs["cs_uri_stem"].str.contains(args.uri_to_filter)
                    df_logs_Filtered = df_logs.where(filter).dropna()
                else:
                    # No Method or URI string specified - no filter to apply!!
                    df_logs_Filtered = df_logs

                logging.info("=== RUN TIME TO READ LOG FILES ===>: " + Util.timeElapsed(time_ScriptStarted))
                print("=== RUN TIME TO READ LOG FILES ===>: " + Util.timeElapsed(time_ScriptStarted))
                # If we ended up with any data, write it.
                if not df_logs_Filtered.empty:
                    # --------------------
                    # Write data to the DB
                    # --------------------
                    time_ToWriteToDB = Util.get_StartTime()
                    logging.info('{0} log rows about to be written to the database.'.format(str(df_logs_Filtered.shape[0])))
                    print('{0} log rows about to be written to the database.'.format(str(df_logs_Filtered.shape[0])))
                    Util.writeDataFrametoDB(df_logs_Filtered, logstable, conn_string)
                    logging.info("=== RUN TIME TO WRITE LOGS TO DB ===>: " + Util.timeElapsed(time_ToWriteToDB))
                    print("=== RUN TIME TO WRITE LOGS TO DB ===>: " + Util.timeElapsed(time_ToWriteToDB))

                    # ---------------------------------------------
                    # Query for latest numbers and report to a file
                    # ---------------------------------------------
                    # Build the output file name based on the script parameters passed in.
                    reportPath = myConfig['ReportPath']
                    reportCSV = "Report"
                    if len(args.method_to_filter) > 0:
                        reportCSV = reportCSV + "-" + args.method_to_filter
                    if len(args.uri_to_filter) > 0:
                        reportCSV = reportCSV + "-" + args.uri_to_filter.replace("/", "_")
                    reportCSV = reportCSV + ".txt"

                    # Report the latest numbers...
                    reportDF = queryDB(dbname, dbuser, dbpassword, dbhost, logstable, geolocatetable)
                    writeDFtoFile(reportDF, reportPath, reportCSV)

                else:
                    logging.info(f'NO LOG ROWS FOUND FOR WRITING TO DATABASE.')
                    print(f'NO LOG ROWS FOUND FOR WRITING TO DATABASE.')

    except:
        err = Util.capture_exception()
        logging.error(err)

    finally:
        logging.info("=== TOTAL SCRIPT RUN TIME ===>: " + Util.timeElapsed(time_ScriptStarted))
        print("=== TOTAL SCRIPT RUN TIME ===>: " + Util.timeElapsed(time_ScriptStarted))
        logging.info('------------------------- Processing Complete -------------------------')

# # ================================================================================================
# # NOTE - Below, I am just trying to slice and group the data for stats... If we use a
# # database table to store the rows of data, this below may not be needed!
#
# # For more pandas dataframe slicing and dicing examples, please see:
# #        https://kontext.tech/article/1195/azure-app-service-iis-log-analytics-using-pandas
# #        https://realpython.com/pandas-groupby/
# #        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html
# #        https://pandas.pydata.org/docs/reference/api/pandas.core.groupby.DataFrameGroupBy.value_counts.html
#
# # Examples of grouping the TrainMat rows by URI and/or client IP...
# # n_by_url = df_logs_Filtered.groupby("cs-uri-stem")["c-ip"].count()
# # n_by_url = df_logs_Filtered.groupby(["cs-uri-stem", "c-ip"])["c-ip"].count()
# # logging.info("### Count of GET requests for SAR Training Material Rows ###")
# # logging.info(n_by_url)
#
# # This gets the total number of requests per URI. (Includes duplicate requests from same IPs)
# # df_urls_byIP = df_logs_Filtered[['cs-uri-stem']].groupby("cs-uri-stem").value_counts(ascending=True)
#
# # This gets the total number of requests per IP per URI. (Also includes duplicate requests
# # from same IPs, but breaks them out!)
# df_urls_byIP = df_logs_Filtered[['cs-uri-stem', 'c-ip']].groupby("cs-uri-stem").value_counts(ascending=True)
# logging.info("### SAR Training Material GET Requests counted by total IPs ###")
# logging.info(df_urls_byIP)
# df_urls_byIP.to_excel('IISStats.xlsx', sheet_name="URI_by_UniqueIPCount")
# # ================================================================================================

