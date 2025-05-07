import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from typing import Tuple

def list_files(directory: str, arr_dep: str) -> list[str]:
    """
    List files in a directory that match a given suffix (e.g., 'arrivals.csv' or 'departures.csv').

    Parameters:
        directory (str): The path to the directory to search.
        arr_dep (str): File suffix to filter by.

    Returns:
        list[str]: A list of filenames that match the suffix.
    """
    matching_files = [
        filename for filename in os.listdir(directory) if filename.endswith(arr_dep)
    ]
    return matching_files

def load_flight_data(airport: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load and filter flight data for a given airport."""
    base_path = os.path.join("..", "..", "data", airport)
    departure_files = list_files(base_path, "departures.parquet")
    arrival_files = list_files(base_path, "arrivals.parquet")

    if not departure_files or not arrival_files:
        raise FileNotFoundError("Missing departure or arrival parquet files.")

    departures = pd.read_parquet(os.path.join(base_path, departure_files[0]))
    arrivals = pd.read_parquet(os.path.join(base_path, arrival_files[0]))
    return filter_flights(departures), filter_flights(arrivals)


def add_scheduled_out_date_dimensions(departures: pd.DataFrame) -> pd.DataFrame:
    """
    Add date and time-related features based on the 'scheduled_out_utc' timestamp for departures.

    Parameters:
        departures (pd.DataFrame): DataFrame containing a 'scheduled_out_utc' column.

    Returns:
        pd.DataFrame: Modified DataFrame with additional time-based columns.
    """
    departures['scheduled_out_utc'] = pd.to_datetime(departures['scheduled_out_utc'])
    departures['scheduled_date'] = departures['scheduled_out_utc'].dt.date
    departures['scheduled_weekday'] = departures['scheduled_out_utc'].dt.day_name()
    departures['scheduled_hour'] = departures['scheduled_out_utc'].dt.hour
    departures['scheduled_month'] = departures['scheduled_out_utc'].dt.month
    departures['scheduled_year'] = departures['scheduled_out_utc'].dt.year
    departures['scheduled_week'] = departures['scheduled_out_utc'].dt.isocalendar().week
    departures['scheduled_day'] = departures['scheduled_out_utc'].dt.day
    departures['scheduled_month_day'] = departures['scheduled_out_utc'].dt.strftime('%m-%d')
    departures['scheduled_weekday_hour'] = departures['scheduled_out_utc'].dt.strftime('%A %H:00')
    return departures


def filter_departures(departures: pd.DataFrame) -> pd.DataFrame:
    """
    Filter the departures DataFrame to only include data for the most frequent scheduled date.

    Parameters:
        departures (pd.DataFrame): DataFrame of departure flights.

    Returns:
        pd.DataFrame: Filtered DataFrame for the most frequent scheduled date.
    """
    enriched = add_scheduled_out_date_dimensions(departures)
    fetch_date = enriched['scheduled_date'].value_counts().idxmax()
    filtered = enriched[enriched['scheduled_date'] == fetch_date]
    return filtered


def add_scheduled_in_date_dimensions(arrivals: pd.DataFrame) -> pd.DataFrame:
    """
    Add date and time-related features based on the 'scheduled_in_utc' timestamp for arrivals.

    Parameters:
        arrivals (pd.DataFrame): DataFrame containing a 'scheduled_in_utc' column.

    Returns:
        pd.DataFrame: Modified DataFrame with additional time-based columns.
    """
    arrivals['scheduled_in_utc'] = pd.to_datetime(arrivals['scheduled_in_utc'])
    arrivals['scheduled_date'] = arrivals['scheduled_out_utc'].dt.date
    arrivals['scheduled_weekday'] = arrivals['scheduled_out_utc'].dt.day_name()
    arrivals['scheduled_hour'] = arrivals['scheduled_out_utc'].dt.hour
    arrivals['scheduled_month'] = arrivals['scheduled_out_utc'].dt.month
    arrivals['scheduled_year'] = arrivals['scheduled_out_utc'].dt.year
    arrivals['scheduled_week'] = arrivals['scheduled_out_utc'].dt.isocalendar().week
    arrivals['scheduled_day'] = arrivals['scheduled_out_utc'].dt.day
    arrivals['scheduled_month_day'] = arrivals['scheduled_out_utc'].dt.strftime('%m-%d')
    arrivals['scheduled_weekday_hour'] = arrivals['scheduled_out_utc'].dt.strftime('%A %H:00')
    return arrivals


def filter_flights(arrivals: pd.DataFrame) -> pd.DataFrame:
    """
    Filter the arrivals DataFrame to only include data for the most frequent scheduled date.

    Parameters:
        arrivals (pd.DataFrame): DataFrame of arrival flights.

    Returns:
        pd.DataFrame: Filtered DataFrame for the most frequent scheduled date.
    """
    enriched = add_scheduled_in_date_dimensions(arrivals)
    fetch_date = enriched['scheduled_date'].value_counts().idxmax()
    filtered = enriched[enriched['scheduled_date'] == fetch_date]
    return filtered
