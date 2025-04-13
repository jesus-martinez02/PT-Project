import pandas as pd
import gtfs_realtime_pb2
from datetime import datetime
import pytz
import os

## Function to generate static data
def generate_static_data(static_path, in_relevant_lines):
    # Read static files
    trips_df = pd.read_csv(static_path + "/trips.txt")
    routes_df = pd.read_csv(static_path + "/routes.txt")
    stops_df = pd.read_csv(static_path + "/stops.txt")
    stop_times_df = pd.read_csv(static_path + "/stop_times.txt")

    trips_df['trip_id'] = trips_df['trip_id'].astype(str)
    trips_df['route_id'] = trips_df['route_id'].astype(str)
    routes_df['route_id'] = routes_df['route_id'].astype(str)
    stops_df['stop_id'] = stops_df['stop_id'].astype(str)
    stops_df['parent_station'] = stops_df['parent_station'].astype(str)
    stops_df['platform_code'] = stops_df['platform_code'].astype(str)


    stop_times_df['stop_id'] = stop_times_df['stop_id'].astype(str)
    stop_times_df['trip_id'] = stop_times_df['trip_id'].astype(str)

    relevant_routes_df = routes_df[routes_df['route_short_name'].isin(in_relevant_lines)]
    trips_routes_merged_df = pd.merge(relevant_routes_df, trips_df)[['route_short_name', 'trip_id', 'service_id']]

    merged_step_1_df = pd.merge(trips_routes_merged_df, stop_times_df)

    #return pd.merge(merged_step_1_df, stops_df)[
    #    ['route_short_name', 'stop_name', 'arrival_time', 'departure_time', 'stop_headsign',
    #     'stop_sequence', 'trip_id', 'stop_id', 'service_id','shape_dist_traveled']]
    return pd.merge(merged_step_1_df, stops_df)

## Processing of a single .pb file
def process_real_time_file(file_path, file_name,in_static_df = None,merge_static = True):
    rows = []
    feed = gtfs_realtime_pb2.FeedMessage()

    with open(file_path, "rb") as f:
        feed.ParseFromString(f.read())

    # Extract each trip and stop_time_update into a flat row
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            trip = entity.trip_update.trip
            trip_id = trip.trip_id
            start_date = trip.start_date
            schedule_relationship = trip.schedule_relationship

            for stu in entity.trip_update.stop_time_update:
                stop_sequence = stu.stop_sequence
                stop_id = stu.stop_id

                arrival_time = stu.arrival.time if stu.HasField("arrival") else None
                arrival_delay = stu.arrival.delay if stu.HasField("arrival") else None

                departure_time = stu.departure.time if stu.HasField("departure") else None
                departure_delay = stu.departure.delay if stu.HasField("departure") else None

                # Convert UNIX timestamps to readable format if available
                arrival_dt = datetime.fromtimestamp(arrival_time, pytz.timezone("Europe/Stockholm")).strftime(
                    '%Y-%m-%d %H:%M:%S') if arrival_time else None
                departure_dt = datetime.fromtimestamp(departure_time, pytz.timezone("Europe/Stockholm")).strftime(
                    '%Y-%m-%d %H:%M:%S') if departure_time else None

                rows.append({
                    "trip_id": trip_id,
                    "start_date": start_date,
                    "schedule_relationship": schedule_relationship,
                    "stop_sequence": stop_sequence,
                    "stop_id": stop_id,
                    # "arrival_time_unix": arrival_time,
                    "arrival_time": arrival_dt,
                    "arrival_delay": arrival_delay,
                    # "departure_time_unix": departure_time,
                    "departure_time": departure_dt,
                    "departure_delay": departure_delay
                })
    temp_rt_df = pd.DataFrame(rows)

    if merge_static:
        df =  pd.merge(temp_rt_df, in_static_df, on=['trip_id', 'stop_id'], suffixes=('_real', '_sched'))
    else:
        df = temp_rt_df

    df['file'] = file_name
    return df

## Read a directory of .pb files contained in 1h
def read_pb_hour(in_pb_path,in_static_path,in_relevant_lines):
    full_df = pd.DataFrame()

    static_df = generate_static_data(static_path = in_static_path, in_relevant_lines = in_relevant_lines)
    for pb_file in os.listdir(in_pb_path):
        new_df = process_real_time_file(file_path = in_pb_path + '/' + pb_file,
                                        file_name = pb_file, in_static_df=static_df)
        full_df = pd.concat([full_df, new_df])

    return full_df

## Read each of the hours within a day of real time data
def read_pb_day(in_day_path,in_static_path,in_relevant_lines):
    year = in_day_path[20:24]
    month = in_day_path[25:27]
    day = in_day_path[28:30]

    final_day_path = in_day_path + '/sl/TripUpdates/' + year + '/' + month + '/' + day
    full_df = pd.DataFrame()
    for hour_path in os.listdir(final_day_path):
        new_df = read_pb_hour(final_day_path + '/' + hour_path,in_static_path, in_relevant_lines)
        full_df = pd.concat([full_df, new_df])
        print(hour_path)

    last_record_df = full_df.drop_duplicates(subset = ['stop_id','trip_id','route_short_name'], keep = 'last')
    return last_record_df


