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


def merge_day_files(files, output_file_name):
    concat_df = pd.DataFrame()
    for file in files:
        df = pd.read_csv(file)
        concat_df = pd.concat([concat_df, df], ignore_index = True)

    concat_df.to_csv(output_file_name,index=False)

def my_read_df(input_path):
    df = pd.read_csv(input_path)
    processed_df = add_df_features(df)
    return processed_df

def add_df_features(input_df):
    column_list = ['trip_id','stop_id','route_short_name','stop_name','stop_headsign' ,'stop_sequence_real', 'arrival_time_real',
                   'departure_time_real', 'arrival_delay','departure_delay',
                   'departure_time_sched','arrival_time_sched','service_id','start_date','shape_dist_traveled'] ## Only get the subset of columns we want to use

    input_df['trip_id'] = input_df['trip_id'].astype(str)
    input_df['stop_id'] = input_df['stop_id'].astype(str)


    step_1_df = input_df[column_list].copy()

    ordered_df = step_1_df.sort_values(by=['start_date','trip_id','stop_sequence_real'], ascending = True)

    ordered_df['distance_between_stops'] = ordered_df.groupby('trip_id')['shape_dist_traveled'].diff()

    ordered_df['arrival_time_real'] = pd.to_datetime(ordered_df['arrival_time_real'], errors='coerce')
    ordered_df['departure_time_real'] = pd.to_datetime(ordered_df['departure_time_real'], errors='coerce')

    ordered_df['prev_departure_real'] = ordered_df.groupby('trip_id')['departure_time_real'].shift(1)

    ordered_df['travel_time'] = (ordered_df['arrival_time_real'] - ordered_df['prev_departure_real']).dt.total_seconds()

    ordered_df['speed'] = (ordered_df['distance_between_stops'] / 1000) / (ordered_df['travel_time'] / 3600)

    return ordered_df