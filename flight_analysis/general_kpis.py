import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from scipy.signal import find_peaks


def decimal_minutes_to_hhmm(decimal_minutes: float) -> str:
    """
    Convert decimal minutes to HH:MM string format.

    Parameters:
        decimal_minutes (float): Time in decimal minutes.

    Returns:
        str: Time in HH:MM format.
    """
    hours = int(decimal_minutes // 60)
    minutes = int(round(decimal_minutes % 60))
    return f"{hours:02d}:{minutes:02d}"


def get_delayed_arrivals_percentage(arrivals: pd.DataFrame) -> tuple[float, float]:
    """
    Calculate percentage of arrivals delayed by more than 1 minute and more than 15 minutes.

    Parameters:
        arrivals (pd.DataFrame): DataFrame containing 'arrival_delay' in seconds.

    Returns:
        tuple[float, float]: Percent delayed over 1 min and over 15 min.
    """
    arrivals = arrivals.copy()
    arrivals['arrival_delay_min'] = arrivals['arrival_delay'] / 60
    delayed_1min = arrivals[arrivals['arrival_delay_min'] > 1]
    delayed_15min = arrivals[arrivals['arrival_delay_min'] > 15]

    perc_1min = round(len(delayed_1min) / len(arrivals) * 100, 2)
    perc_15min = round(len(delayed_15min) / len(arrivals) * 100, 2)
    return perc_1min, perc_15min


def get_departure_delay_percentage(departures: pd.DataFrame) -> tuple[float, float]:
    """
    Calculate percentage of departures delayed by more than 1 minute and more than 15 minutes.

    Parameters:
        departures (pd.DataFrame): DataFrame containing 'departure_delay' in seconds.

    Returns:
        tuple[float, float]: Percent delayed over 1 min and over 15 min.
    """
    departures = departures.copy()
    departures['departure_delay_min'] = departures['departure_delay'] / 60
    delayed_1min = departures[departures['departure_delay_min'] > 1]
    delayed_15min = departures[departures['departure_delay_min'] > 15]

    perc_1min = round(len(delayed_1min) / len(departures) * 100, 2)
    perc_15min = round(len(delayed_15min) / len(departures) * 100, 2)
    return perc_1min, perc_15min


def get_general_metrics(flights: pd.DataFrame) -> dict:
    """
    Compute general KPI metrics for a flight dataset.

    Parameters:
        flights (pd.DataFrame): DataFrame of flight records.

    Returns:
        dict: KPI metrics including counts and percentages of completed, cancelled, blocked, and diverted flights.
    """
    total = flights['fa_flight_id'].nunique()
    completed = len(flights[(~flights['blocked']) & (~flights['diverted']) & (~flights['cancelled'])])
    blocked = flights['blocked'].sum()
    cancelled = flights['cancelled'].sum()
    diverted = flights['diverted'].sum()

    return {
        'total': total,
        'completed': completed,
        'blocked': blocked,
        'cancelled': cancelled,
        'diverted': diverted,
        'percent_completed': completed / total * 100,
        'percent_cancelled': cancelled / total * 100,
        'percent_diverted': diverted / total * 100
    }


def get_arrival_delay_metrics(arrivals: pd.DataFrame) -> dict:
    """
    Calculate delay statistics for arrival flights.

    Parameters:
        arrivals (pd.DataFrame): DataFrame containing arrival delay data.

    Returns:
        dict: KPI dictionary with total flights, average, max, min, median delay, and delay percentages.
    """
    total = arrivals['fa_flight_id'].nunique()
    avg = arrivals['arrival_delay'].sum() / total / 60
    max_ = arrivals['arrival_delay'].max() / 60
    min_ = arrivals['arrival_delay'].min() / 60
    median = arrivals['arrival_delay'].median() / 60

    perc_1min, perc_15min = get_delayed_arrivals_percentage(arrivals)

    return {
        'total': total,
        'avg_delay_min': avg,
        'max_delay_min': max_,
        'min_delay_min': min_,
        'median_delay_min': median,
        'delay_percentage': perc_1min,
        'delay_percentage_15min': perc_15min
    }


def get_departure_delay_metrics(departures: pd.DataFrame) -> dict:
    """
    Calculate delay statistics for departure flights.

    Parameters:
        departures (pd.DataFrame): DataFrame containing departure delay data.

    Returns:
        dict: KPI dictionary with total flights, average, max, min, median delay, and delay percentages.
    """
    total = departures['fa_flight_id'].nunique()
    avg = departures['departure_delay'].sum() / total / 60
    max_ = departures['departure_delay'].max() / 60
    min_ = departures['departure_delay'].min() / 60
    median = departures['departure_delay'].median() / 60

    perc_1min, perc_15min = get_departure_delay_percentage(departures)

    return {
        'total': total,
        'avg_delay_min': avg,
        'max_delay_min': max_,
        'min_delay_min': min_,
        'median_delay_min': median,
        'delay_percentage': perc_1min,
        'delay_percentage_15min': perc_15min
    }


def identify_peaks(flights_per_hour: pd.DataFrame, arr_dep: str) -> pd.DataFrame:
    """
    Identify peak flight periods in hourly flight data using signal peak detection.

    Parameters:
        flights_per_hour (pd.DataFrame): DataFrame containing hourly flight counts.
        arr_dep (str): Either 'arrivals' or 'departures', used for column naming.

    Returns:
        pd.DataFrame: Updated DataFrame with peak detection boolean column.
    """
    col_num = f'num_{arr_dep}'
    col_avg = f'avg_{arr_dep}'
    col_peak = f'{arr_dep}_peak'

    flights_per_hour[col_avg] = flights_per_hour[col_num].mean()
    peaks, _ = find_peaks(flights_per_hour[col_num], distance=3)

    flights_per_hour[col_peak] = False
    flights_per_hour.loc[peaks, col_peak] = True
    flights_per_hour[col_peak] = np.where(
        (flights_per_hour[col_peak]) & (flights_per_hour[col_num] > flights_per_hour[col_avg]),
        True, False
    )
    return flights_per_hour


def get_flights_per_hour(arrivals: pd.DataFrame, departures: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate arrivals and departures per hour with delay metrics and combined total.

    Parameters:
        arrivals (pd.DataFrame): Arrival flights data.
        departures (pd.DataFrame): Departure flights data.

    Returns:
        pd.DataFrame: Hourly summary of flights, delays, and totals.
    """
    hours_df = pd.DataFrame({'scheduled_hour': range(24)})

    arrivals_hourly = (
        arrivals.groupby('scheduled_hour')
        .agg({'fa_flight_id': 'count', 'arrival_delay': 'mean'})
        .rename(columns={'fa_flight_id': 'num_arrivals', 'arrival_delay': 'avg_arrival_delay'})
        .reset_index()
    )

    departures_hourly = (
        departures.groupby('scheduled_hour')
        .agg({'fa_flight_id': 'count', 'departure_delay': 'mean'})
        .rename(columns={'fa_flight_id': 'num_departures', 'departure_delay': 'avg_departure_delay'})
        .reset_index()
    )

    hourly = hours_df.merge(arrivals_hourly, on='scheduled_hour', how='left')
    hourly = hourly.merge(departures_hourly, on='scheduled_hour', how='left')

    hourly['avg_arrival_delay_min'] = round(hourly['avg_arrival_delay'] / 60, 2).fillna(0)
    hourly['avg_departure_delay_min'] = round(hourly['avg_departure_delay'] / 60, 2).fillna(0)
    hourly['num_arrivals'] = hourly['num_arrivals'].fillna(0).astype(int)
    hourly['num_departures'] = hourly['num_departures'].fillna(0).astype(int)
    hourly['total_flights'] = hourly['num_arrivals'] + hourly['num_departures']

    hourly['hour_hh_mm'] = (hourly['scheduled_hour'] * 60).apply(decimal_minutes_to_hhmm)

    return hourly
