#
# Lance Gilliland - May 1, 2024
# This script reads unique IP addresses that have been added to a table (iis_logs.sar) and compares against the
#  IPs already geolocated within an IP geolocation table (iis_logs.geolocation).  For any new IPs found that are
#  not already geolocated, the script will geolocate them and add them to the geolocation table.
# Script parameters:
#   1) -l (optional) a string to denote the type of logging that this script will write. (defaults to INFO)
# A pickle file with the database connection and table info is required.
# In addition to some standard libraries, this script required the install of psycopg2 and geoip2
#

import Util
import logging
import argparse  # required for processing command line arguments
import os  # required for checking if path/file exists
import pickle  # for reading a pickle file
import datetime  # for working with dates and times

import pandas as pd  # used to read, store and analyze csv file info
import psycopg2  # used for database stuff
import geoip2.database  # used for IP lookup
import geoip2.errors  # used for IP lookup errors
import requests  # use for the IP lookup service hosted by SERVIR!  geolocate_usingSERVIRService()


# Set up the argparser to capture any arguments...
def setupArgs():
    parser = argparse.ArgumentParser(__file__,
                                     description="This function reads IPs from a table in the DB that have not"
                                                 " been geolocated, geolocates each IP, and write the info to"
                                                 " another table.")
    parser.add_argument("-l", "--logging",
                        help="Logging level to report. Default: INFO",
                        type=str, choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")

    return parser.parse_args()


def getIPsToLookup(dbname, dbuser, dbpassword, dbhost, logstable, geolocatetable):
    # Function queries to select unique IPs from sar where IPs not in (select IPs from geolocation).
    # This will ensure that we do not try to geolocate any IPs that we have already captured.
    # If no error, but simply no rows found, return an empty list!
    # Returns a list of IPs, or an empty list.
    try:
        returnList = []

        # Create a psycopg2 db connection with the params passed in
        conn = psycopg2.connect(dbname=dbname, user=dbuser, password=dbpassword, host=dbhost)

        # Create a cursor object
        cur = conn.cursor()

        # Read the latest date and related time from the table
        cur.execute(f"SELECT DISTINCT c_ip FROM {logstable} WHERE c_ip NOT IN "
                    f"(SELECT ip FROM {geolocatetable})")
        rows = cur.fetchall()
        for row in rows:
            returnList.append(row[0])

        # Close the cursor and connection
        cur.close()
        conn.close()

        return returnList

    except psycopg2.Error as e:
        logging.error("### Error connecting to database: ", e)
        print("### Error connecting to database: ", e)
        return returnList

    except Exception as e:
        logging.error('### Error occurred in GetIPsToLookup() ###, %s' % e)
        print('### Error occurred in GetIPsToLookup() ###, %s' % e)
        return returnList


def geolocateIP(theIP, geoip2Reader):
    # Uses geoip2 database to lookup the passed in IP
    # https://dev.maxmind.com/geoip/geolite2-free-geolocation-data
    # https://geoip2.readthedocs.io/en/latest/#database-example
    try:
        returnList = []

        try:
            country_name = ""
            response = geoip2Reader.country(theIP)
            country_name = response.country.name   # Not clear why, but this sometimes raises an error...
            if len(country_name) > 0:               # and sometimes returns None. So capture both.
                returnList = [ip, country_name]
        except geoip2.errors.AddressNotFoundError as e:
            logging.info("{0} not found in IP database - SKIPPING!".format(theIP))
            print("{0} not found in IP database - SKIPPING!".format(theIP))

        return returnList

    except:
        err = Util.capture_exception()
        logging.error(err)
        logging.error("Error raised looking up IP {0} - Country returned as {1}.".format(theIP, country_name))
        print("Error raised looking up IP {0} - Country returned as {1}.".format(theIP, country_name))
        return returnList


# -----------------------------------
# Not Currently Used, but could be!!!
# -----------------------------------
def geolocate_usingSERVIRService(theIP):
    # This function makes a GET request to the SERVIR-exposed IP Geolocator service.
    # Return JSON values should be:
    # {"country": {"country_code": "BR", "country_name": "Brazil"}}
    #   or possibly
    # {"country": {"country_code": None, "country_name": None}}
    # {"country": "Not Found"} ???
    try:
        returnList = []
        country_name = ""

        url = "https://locator.servirglobal.net/get_country/"
        params = {"ip_address": theIP}

        # response = requests.get(url, params=params)
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data:
                # Get the country_name value and check it - it could be None or "Not Found"!  :(
                country_name = data['country']['country_name']
                if country_name:
                    if country_name == "Not Found":
                        logging.info("IP {0} not found in IP database - SKIPPING!".format(theIP))
                        print("IP {0} not found in IP database - SKIPPING!".format(theIP))
                    else:
                        # The country name was returned, build the return object
                        returnList = [ip, country_name]
                else:
                    # The weird thing where the IP call returns None for the country_name.
                    logging.error("IP: {0} - Country returned from geolocation service as None.".format(theIP))
                    print("IP: {0} - Country returned from geolocation service as None.".format(theIP))
            else:
                logging.error("IP {0} - No data returned from URL request.".format(theIP))
                print("IP {0} - No data returned from URL request.".format(theIP))
        else:
            logging.error("Geolocate service failed to fetch data: Status {0}".format(str(response.status_code)))
            print("Geolocate service failed to fetch data: Status {0}".format(str(response.status_code)))

        return returnList

    except:
        err = Util.capture_exception()
        logging.error(err)
        return returnList


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
        logging.basicConfig(filename=logDir + '\\GeoLocate_' + datetime.date.today().strftime('%Y-%m-%d') + '.log',
                            level=args.logging,
                            format='%(asctime)s: %(levelname)s --- %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p')

        logging.info('------------------------- Processing Starting -------------------------')

        # Get a start time for the script.
        time_ScriptStarted = Util.get_StartTime()

        dbname = myConfig['dbname']
        dbuser = myConfig['dbuser']
        dbpassword = myConfig['dbpassword']
        dbhost = myConfig['dbhost']
        logstable = myConfig['logstable']
        geolocatetable = myConfig['geolocatetable']
        IPDBPathFile = myConfig['IPDBPathFile']

        # Build the connection string for the psycopg2 connection to postgres
        # conn_string = 'postgresql://postgres:@bergson.socrates.work/iis_logs'
        conn_string = 'postgresql://' + dbuser + ':' + dbpassword + '@' + dbhost + '/' + dbname

        # --------------------------------------------------------------------------------------------
        # select unique IPs from sar where IPs not in (select IPs from geolocation)
        lstIPsTolookup = getIPsToLookup(dbname, dbuser, dbpassword, dbhost, logstable, geolocatetable)
        logging.info("{0} IPs found that need to be looked up!".format(str(len(lstIPsTolookup))))
        if len(lstIPsTolookup) > 0:
            # Instantiate the IP reader
            countryReader = geoip2.database.Reader(IPDBPathFile)
            numProcessed = 0
            nestedlstLocatedIPs = []
            for ip in lstIPsTolookup:
                # if numProcessed > 1000:  # We can only do 1000 lookups per day via the service. N/A on local DB.
                #     logging.info("Max limit of 1000 lookups exceeded, run more tomorrow!")
                #     break

                # ------------------------------------------------------------------------------
                # Lookup each IP and return a list [ip, country]
                tmplst = geolocateIP(ip, countryReader)
                # tmplst = geolocate_usingSERVIRService(ip)
                if len(tmplst) > 0:
                    nestedlstLocatedIPs.append(tmplst)
                numProcessed = numProcessed + 1

            if len(nestedlstLocatedIPs) > 0:
                # Convert the nested list to a dataframe
                dfGeolocated = pd.DataFrame(nestedlstLocatedIPs)
                dfGeolocated.columns = ['ip', 'country']

                # ----------------------------------------------------------------------------
                # Call function to insert the IPs from the dataframe to the table.
                logging.info('{0} rows about to be written to the database.'.format(str(dfGeolocated.shape[0])))
                print('{0} log rows about to be written to the database.'.format(str(dfGeolocated.shape[0])))
                Util.writeDataFrametoDB(dfGeolocated, geolocatetable, conn_string)

            else:
                logging.info(f'No successful GeoLocation lookups!')
                print(f'No successful GeoLocation lookups!')

        else:
            logging.info(f'NO IPs FOUND TO LOOKUP.')
            print(f'NO IPs FOUND TO LOOKUP.')

    except:
        err = Util.capture_exception()
        logging.error(err)

    finally:
        logging.info("=== TOTAL SCRIPT RUN TIME ===>: " + Util.timeElapsed(time_ScriptStarted))
        print("=== TOTAL SCRIPT RUN TIME ===>: " + Util.timeElapsed(time_ScriptStarted))
        logging.info('------------------------- Processing Complete -------------------------')
