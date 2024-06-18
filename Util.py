
import time  # used in get_StartTime() and timeElapsed() for getting and reporting script timing
import linecache  # required for capture_exception()
import sys  # required for capture_exception()
from sqlalchemy import create_engine  # used in writing a dataframe to the database


# Common function used by many
def capture_exception():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    s = '### ERROR ### [{}, LINE {} "{}"]: {}'.format(filename, lineno, line.strip(), exc_obj)
    return s


# Get a new time object
def get_StartTime():
    timeStart = time.time()
    return timeStart


# Calculate and return a formatted string with the time elapsed since the input time.
def timeElapsed(timeS):
    seconds = time.time() - timeS
    hours = seconds // 3600
    seconds -= 3600 * hours
    minutes = seconds // 60
    seconds -= 60 * minutes
    if hours == 0 and minutes == 0:
        return "%02d seconds" % seconds
    if hours == 0:
        return "%02d:%02d seconds" % (minutes, seconds)
    return "%02d:%02d:%02d seconds" % (hours, minutes, seconds)


def writeDataFrametoDB(theDF, theTable, db_conn_string):
    # Write the data to the date store...
    try:
        # Create an sqlalchemy db connection based on the connection string passed in
        db = create_engine(db_conn_string)
        conn = db.connect()

        # Write(append) the DF to the table
        theDF.to_sql(theTable, con=conn, if_exists='append', index=False)

        # conn.autocommit = True
        conn.close()

    except Exception:
        raise


