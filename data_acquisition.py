
#from koda_file_processing import *
#from api_connection import *

import pandas as pd
import os

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

def my_read_df(input_path, return_dict = False):
    df = pd.read_csv(input_path)
    processed_df = add_df_features(df)
    if return_dict:
        dict_df = processed_df_to_dict(processed_df)
        return dict_df
    return processed_df

def add_df_features(input_df):
    column_list = ['trip_id','stop_id','route_short_name','stop_name','stop_headsign' ,'stop_sequence_real', 'arrival_time_real',
                   'departure_time_real', 'arrival_delay','departure_delay',
                   'departure_time_sched','arrival_time_sched','service_id','start_date','shape_dist_traveled',
                   'stop_lat','stop_lon'] ## Only get the subset of columns we want to use

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

    ordered_df['dwell_time'] = ordered_df['departure_time_real'] - ordered_df['arrival_time_real']


    list_trip_id = hospital_trips(ordered_df)

    df_annoying = ordered_df.loc[
        (ordered_df['trip_id'].isin(list_trip_id))
        & (
                ~(ordered_df['stop_name'] == 'Södersjukhuset')
                & ~((ordered_df['stop_name'] == 'Rosenlund') & (ordered_df['stop_headsign'] == 'Sickla udde'))
                & ~((ordered_df['stop_name'] == 'Södra station') & (ordered_df['stop_headsign'] == 'Hornsberg'))
        )
        ]

    df_annoying['stop_sequence_real'] = df_annoying.apply(
        lambda row: row['stop_sequence_real'] - 1
        if row['stop_headsign'] not in ['Sickla udde via Södersjukhuset', 'Hornsberg via Södersjukhuset']
        else row['stop_sequence_real'],
        axis=1
    )

    df_annoying['stop_headsign'] = df_annoying['stop_headsign'].apply(
        lambda des: 'Sickla udde' if des == 'Sickla udde via Södersjukhuset' else (
            'Hornsberg' if des == 'Hornsberg via Södersjukhuset' else des))


    regular_trips_df = ordered_df[~ordered_df['trip_id'].isin(list_trip_id)]
    df_clean = pd.concat([df_annoying, regular_trips_df])

    row_df = merge_row_files()

    merged_df = pd.merge(df_clean, row_df)

    return merged_df



def annoying_sequences(u):
    if u['stop_headsign'] not in ['Sickla udde via Södersjukhuset', 'Hornsberg via Södersjukhuset']:
        u['stop_sequence_sched'] -= 1

    return u['stop_sequence_sched']


def hospital_trips(df):
    return list(set(df[(df['stop_name'] == 'Södersjukhuset') | (
        df['stop_headsign'].isin(['Sickla udde via Södersjukhuset', 'Hornsberg via Södersjukhuset']))]['trip_id']))


def processed_df_to_dict(input_df):
    input_df['stop_line_headsign'] = input_df['route_short_name'].astype(str) + '_' + input_df['stop_headsign']
    df_dict = dict.fromkeys(input_df['stop_line_headsign'].unique())
    for destination in df_dict.keys():
        df_dict[destination] = input_df[input_df['stop_line_headsign'] == destination]
    return df_dict

def merge_row_files(row_path = 'data/row'):
    concat_df = pd.DataFrame()
    for file in os.listdir(row_path):
        df = pd.read_csv(row_path + '/' + file, sep = ';')
        df['percentage_row'] = df['percentage_row'].apply(
            lambda x: '1' if x == '1,000,000,000,000,000' else x).str.replace(',', '.').astype(float)
        concat_df = pd.concat([concat_df, df], ignore_index = True)
    return concat_df