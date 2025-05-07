import pandas as pd 
import numpy as np
import requests
import json
import time
from datetime import datetime 
import os
from datetime import datetime
from datetime import datetime, timedelta
import sys
import pytz
from dotenv import load_dotenv
import os



def get_flights_api(apiKey,payload,arr_dep, start_date, end_date,airport):
    try:
        apiUrl = "https://aeroapi.flightaware.com/aeroapi/"
        auth_header = {'x-apikey': apiKey}
        response = requests.get(apiUrl +
                                f"airports/{airport}/flights/{arr_dep}?start={start_date}&end={end_date}",
                                params=payload,
                                headers=auth_header)
        if response.status_code == 200:
            print("API request successful")
            data = response.json()
            return data
        elif response.status_code == 429:
            print("Too many requests. Retrying after 60 seconds...")
            time.sleep(60)
            return get_flights_api(apiKey, payload, arr_dep, start_date, end_date, airport)
        else:
            print(response.status_code)

    except Exception as e:
        print(e)


def get_timeframe_table(start_date, hour_frequency, num_days):
    # Convert start_date string to datetime object
    start_datetime = pd.to_datetime(start_date)

    # Create an empty list to store the datetime ranges
    datetime_ranges = []

    for day in range(num_days):
        # Calculate the current date
        current_date = start_datetime + pd.Timedelta(days=day)
        for hour in range(24 // hour_frequency):
            # Calculate the start and end times for the current hour range
            start_time = current_date + pd.Timedelta(hours=hour * hour_frequency)
            end_time = start_time + pd.Timedelta(hours=hour_frequency, minutes=5)

            # Append the datetime range to the list
            datetime_ranges.append({'start_time': start_time, 'end_time': end_time})

    # Create a DataFrame from the datetime_ranges list
    df = pd.DataFrame(datetime_ranges)

    # Format the 'start_time' and 'end_time' columns as strings in the desired format
    df['start_time'] = df['start_time'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df['end_time'] = df['end_time'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f"Timeframe: {df['start_time'].min()} - {df['end_time'].max()}")
    return df


def get_flights(apiKey, arr_dep, timeframes_df, airport):
    flights_list = []
    for i, row in timeframes_df.iterrows():
        start_date = row['start_time']
        end_date = row['end_time']
        payload = {'max_pages': 5}
        response = get_flights_api(apiKey, payload,arr_dep, start_date, end_date, airport)
        if arr_dep == 'arrivals':
            arrivals = pd.json_normalize(response['arrivals'])
            flights_list.append(arrivals)
        else:
            departures= pd.json_normalize(response['departures'])
            flights_list.append(departures)
        time.sleep(30)

    if flights_list:
        flights_df = pd.concat(flights_list, ignore_index=True)
        return flights_df
    
def get_previous_weekday(weekday_name):
    # Mapping of weekday names to their corresponding weekday numbers
    weekdays = {
        'monday': 1,
        'tuesday': 2,
        'wednesday': 3,
        'thursday': 4,
        'friday': 5,
        'saturday': 6,
        'sunday': 7
    }
    
    # Get the current date and its weekday
    current_date = datetime.now().date()
    current_weekday = current_date.isoweekday()  # 1 (Monday) to 7 (Sunday)
    
    # Check if the provided weekday name is valid
    if weekday_name.lower() not in weekdays:
        raise ValueError("Invalid weekday name. Please enter a valid weekday (e.g., 'monday').")
    
    target_weekday = weekdays[weekday_name.lower()]
    
    # Calculate the difference in days
    days_difference = (current_weekday - target_weekday) % 7
    
    # If the current date is the target weekday, go back one week
    if days_difference == 0:
        days_difference = 7
    
    # Calculate the previous date for the specified weekday
    previous_date = current_date - timedelta(days=days_difference)
    
    return previous_date

def transform_time_columns(flights, time_cols):
    for col in time_cols:
        try:
            flights[col + '_utc'] = pd.to_datetime(flights[col], utc=True)
            flights = flights.drop(columns=[col])
        except Exception as e:
            print(f"Error converting column {col}: {e}")
    return flights

def get_time_dimension_columns(flights, time_cols):
    for col in time_cols:
        flights[col + '_date'] = flights[col + '_utc'].dt.date
        flights[col + '_hour'] = flights[col + '_utc'].dt.hour
        flights[col + '_minute'] = flights[col + '_utc'].dt.minute
    return flights

def convert_utc_to_local(utc_time, tz_name):
    if pd.isna(utc_time) or pd.isna(tz_name):
        return pd.NaT
    try:
        local_tz = pytz.timezone(tz_name)
        return utc_time.tz_convert(local_tz)
    except Exception as e:
        print(f"Error converting {utc_time} with timezone {tz_name}: {e}")
        return pd.NaT

def transform_utc_columns_to_local_time(flights):
    origin_time_columns = ['scheduled_out_utc', 'estimated_out_utc', 'actual_out_utc',
             'scheduled_off_utc', 'estimated_off_utc', 'actual_off_utc']
    desitnation_time_columns = ['scheduled_on_utc', 'estimated_on_utc', 'actual_on_utc',
             'scheduled_in_utc','estimated_in_utc', 'actual_in_utc']

    # Transform origin time columns to local time
    for col in origin_time_columns:
        col_wo_utc = col[:-4]
        col_w_local = col_wo_utc + '_local'
        flights[col_w_local] = flights.apply(lambda row: convert_utc_to_local(row[col], row['origin.timezone']),axis=1)
    # Transform destination time columns to local time
    for col in desitnation_time_columns:
        col_wo_utc = col[:-4]
        col_w_local = col_wo_utc + '_local'
        flights[col_w_local] = flights.apply(lambda row: convert_utc_to_local(row[col], row['destination.timezone']),axis=1)        
    return flights


def save_dataframe_to_parquet(df, directory, filename):
    
    # Create the directory if it doesn't exist
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Save the DataFrame to a Parquet file
    file_path = f"{directory}/{filename}"
    df.to_parquet(file_path)
    print(f"DataFrame saved to: {file_path}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python flights_ingestion.py <weekday> <airport_code>")
        sys.exit(1)

    start_time = time.time()

    # Load parameter
    arr_dep = sys.argv[1].lower()
    weekday = sys.argv[2].title()
    airport_code = sys.argv[3].upper()

    # Load API key from environment variable
    load_dotenv()  # Load .env file
    apiKey = os.getenv('FLIGHTAWARE_KEY')

    # Define time columns 
    time_cols = ['scheduled_out', 'estimated_out', 'actual_out',
             'scheduled_off', 'estimated_off', 'actual_off',
             'scheduled_on', 'estimated_on', 'actual_on',
             'scheduled_in','estimated_in', 'actual_in']

    # Get current and fetch date for flights
    current_date = datetime.now().date()
    result_date = get_previous_weekday(weekday)

    # Define timeframes for API calls  
    timeframes_df = get_timeframe_table(start_date=result_date, hour_frequency=2, num_days=1)

    # Get flights 
    flights_raw = get_flights(apiKey, arr_dep, timeframes_df, airport=airport_code)
    flights = flights_raw.pipe(transform_time_columns, time_cols).drop_duplicates(subset='fa_flight_id')
    flights_w_utc_local_time = transform_utc_columns_to_local_time(flights)
    
    # Cleanup column names 
    transformed_column_names = [column.replace('.', '_') for column in list(flights_w_utc_local_time.columns)]
    flights_w_utc_local_time.columns = transformed_column_names
    final_flights = flights_w_utc_local_time.copy() 

    # Save data 
    file_path = f"../data/{airport_code}/"
    file_name = f"{result_date}_{airport_code.upper()}_{arr_dep}.parquet"
    save_dataframe_to_parquet(final_flights, file_path, file_name)


    end_time = time.time()
    runtime = end_time - start_time

    print(f"Script finished in {runtime:.2f} seconds")