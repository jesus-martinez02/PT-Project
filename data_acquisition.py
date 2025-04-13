from koda_file_processing import *
from api_connection import *

## Global function to manage the download of 1 day of static and real time data,
## processing all the .pb files and returning a csv with the collected data
def get_day(day:str, api_key:str, in_relevant_lines):
    real_time_path = 'sl-TripUpdates-' + day
    static_path = 'GTFS-SL-' + day
    download_day_data(day, api_key, real_time_path, static_path)

    df = read_pb_day(in_day_path = "data/" + real_time_path, in_static_path = "data/" + static_path,in_relevant_lines = in_relevant_lines)

    df.to_csv(day + ".csv",index = False)