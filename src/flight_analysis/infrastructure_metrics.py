import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def get_departures_per_terminal(filtered_departures: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the number and utilization percentage of departures per terminal.

    Parameters:
        filtered_departures (pd.DataFrame): Preprocessed DataFrame of departure flights.

    Returns:
        pd.DataFrame: Terminal-level departure count and utilization metrics.
    """
    df = filtered_departures.groupby('terminal_origin')['fa_flight_id'].count().reset_index()
    df.columns = ['terminal', 'num_departures']
    df['utilization_perc'] = round(df['num_departures'] / df['num_departures'].sum() * 100, 2)
    return df.sort_values('num_departures', ascending=False).reset_index(drop=True)


def get_terminal_departure_delays(filtered_departures: pd.DataFrame) -> pd.DataFrame:
    """
    Compute total and average departure delays per terminal.

    Parameters:
        filtered_departures (pd.DataFrame): Preprocessed DataFrame of departure flights.

    Returns:
        pd.DataFrame: Terminal-level delay metrics including total and average delay in minutes.
    """
    df = filtered_departures.groupby('terminal_origin').agg({
        'departure_delay': 'sum',
        'fa_flight_id': 'count'
    }).reset_index()

    df['total_departure_delay_min'] = df['departure_delay'] / 60
    df['avg_departure_delay_min'] = round(df['departure_delay'] / df['fa_flight_id'] / 60, 2)
    df = df.drop(columns='departure_delay')
    df.columns = ['terminal', 'num_departures', 'total_departure_delay_min', 'avg_departure_delay_min']
    return df


def get_arrivals_per_terminal(filtered_arrivals: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the number and utilization percentage of arrivals per terminal.

    Parameters:
        filtered_arrivals (pd.DataFrame): Preprocessed DataFrame of arrival flights.

    Returns:
        pd.DataFrame: Terminal-level arrival count and utilization metrics.
    """
    df = filtered_arrivals.groupby('terminal_destination')['fa_flight_id'].count().reset_index()
    df.columns = ['terminal', 'num_arrivals']
    df['utilization_perc'] = round(df['num_arrivals'] / df['num_arrivals'].sum() * 100, 2)
    return df.sort_values('num_arrivals', ascending=False).reset_index(drop=True)


def get_terminal_arrival_delays(filtered_arrivals: pd.DataFrame) -> pd.DataFrame:
    """
    Compute total and average arrival delays per terminal.

    Parameters:
        filtered_arrivals (pd.DataFrame): Preprocessed DataFrame of arrival flights.

    Returns:
        pd.DataFrame: Terminal-level delay metrics including total and average delay in minutes.
    """
    df = filtered_arrivals.groupby('terminal_origin').agg({
        'arrival_delay': 'sum',
        'fa_flight_id': 'count'
    }).reset_index()

    df['total_arrival_delay_min'] = df['arrival_delay'] / 60
    df['avg_arrival_delay_min'] = round(df['arrival_delay'] / df['fa_flight_id'] / 60, 2)
    df = df.drop(columns='arrival_delay')
    df.columns = ['terminal', 'num_arrivals', 'total_arrival_delay_min', 'avg_arrival_delay_min']
    return df


def get_departures_per_gate(filtered_departures: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the number and utilization percentage of departures per gate.

    Parameters:
        filtered_departures (pd.DataFrame): Preprocessed DataFrame of departure flights.

    Returns:
        pd.DataFrame: Gate-level departure count and utilization metrics.
    """
    df = filtered_departures.groupby('gate_origin')['fa_flight_id'].count().reset_index()
    df.columns = ['gate', 'num_departures']
    df['utilization_perc'] = round(df['num_departures'] / df['num_departures'].sum() * 100, 2)
    return df.sort_values('num_departures', ascending=False).reset_index(drop=True)


def get_arrivals_per_gate(filtered_arrivals: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the number and utilization percentage of arrivals per gate.

    Parameters:
        filtered_arrivals (pd.DataFrame): Preprocessed DataFrame of arrival flights.

    Returns:
        pd.DataFrame: Gate-level arrival count and utilization metrics.
    """
    df = filtered_arrivals.groupby('gate_destination')['fa_flight_id'].count().reset_index()
    df.columns = ['gate', 'num_arrivals']
    df['utilization_perc'] = round(df['num_arrivals'] / df['num_arrivals'].sum() * 100, 2)
    return df.sort_values('num_arrivals', ascending=False).reset_index(drop=True)


def get_departures_per_runway(filtered_departures: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the number and utilization percentage of departures per runway.

    Parameters:
        filtered_departures (pd.DataFrame): Preprocessed DataFrame of departure flights.

    Returns:
        pd.DataFrame: Runway-level departure count and utilization metrics.
    """
    df = filtered_departures.groupby('actual_runway_off')['fa_flight_id'].count().reset_index()
    df.columns = ['runway', 'num_departures']
    df['utilization_perc'] = round(df['num_departures'] / df['num_departures'].sum() * 100, 2)
    return df.sort_values('num_departures', ascending=False).reset_index(drop=True)


def get_arrivals_per_runway(filtered_arrivals: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the number and utilization percentage of arrivals per runway.

    Parameters:
        filtered_arrivals (pd.DataFrame): Preprocessed DataFrame of arrival flights.

    Returns:
        pd.DataFrame: Runway-level arrival count and utilization metrics.
    """
    df = filtered_arrivals.groupby('actual_runway_on')['fa_flight_id'].count().reset_index()
    df.columns = ['runway', 'num_arrivals']
    df['utilization_perc'] = round(df['num_arrivals'] / df['num_arrivals'].sum() * 100, 2)
    return df.sort_values('num_arrivals', ascending=False).reset_index(drop=True)
