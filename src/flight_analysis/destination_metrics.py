import pandas as pd 
import numpy as np
from datetime import datetime, timedelta

def decimal_minutes_to_hhmm(decimal_minutes):
    """
    Converts a decimal minute value to HH:MM time format.

    Args:
        decimal_minutes (float): Time in decimal minutes.

    Returns:
        str: Time formatted as HH:MM.
    """
    hours = int(decimal_minutes // 60)
    minutes = int(round(decimal_minutes % 60))
    return f"{hours:02d}:{minutes:02d}"


def get_departure_destinations(departures):
    """
    Aggregates and summarizes departure data by destination airport.

    Args:
        departures (pd.DataFrame): DataFrame with departure flight data.

    Returns:
        pd.DataFrame: Summary of departures per destination including route distance and estimated time enroute.
    """
    flights_per_destination = departures.groupby(by=['destination_code_icao','destination_code_iata', 'destination_name', 'destination_city'])\
        .agg({'fa_flight_id': 'count', 'route_distance': 'mean', 'filed_ete': 'mean'}).reset_index()
    
    flights_per_destination.columns = ['dep_airport_icao', 'dep_airport_iata', 'dep_airport_name', 'dep_city', 'num_departures', 'dep_route_distance_miles', 'dep_ete_duration_s']
    flights_per_destination['dep_ete_duration_min'] = round(flights_per_destination['dep_ete_duration_s'] / 60, 2)
    flights_per_destination['dep_ete_duration_hh_mm'] = flights_per_destination['dep_ete_duration_min'].apply(decimal_minutes_to_hhmm)
    flights_per_destination = flights_per_destination.sort_values(by=['num_departures'], ascending=False)
    return flights_per_destination


def get_arrival_destinations(arrivals):
    """
    Aggregates and summarizes arrival data by origin airport.

    Args:
        arrivals (pd.DataFrame): DataFrame with arrival flight data.

    Returns:
        pd.DataFrame: Summary of arrivals per origin including route distance and estimated time enroute.
    """
    flights_per_destination = arrivals.groupby(by=['origin_code_icao','origin_code_iata', 'origin_name', 'origin_city'])\
        .agg({'fa_flight_id': 'count', 'route_distance': 'mean', 'filed_ete': 'mean'}).reset_index()
    
    flights_per_destination.columns = ['arr_airport_icao', 'arr_airport_iata', 'arr_airport_name', 'arr_city', 'num_arrivals', 'arr_route_distance_miles', 'arr_ete_duration_s']
    flights_per_destination['arr_ete_duration_min'] = round(flights_per_destination['arr_ete_duration_s'] / 60, 2)
    flights_per_destination['arr_ete_duration_hh_mm'] = flights_per_destination['arr_ete_duration_min'].apply(decimal_minutes_to_hhmm)
    flights_per_destination = flights_per_destination.sort_values(by=['num_arrivals'], ascending=False)
    return flights_per_destination


def get_all_destinations(departures, arrivals):
    """
    Combines and aggregates arrival and departure destination data into a single destination summary.

    Args:
        departures (pd.DataFrame): Departure destination summary (from `get_departure_destinations`).
        arrivals (pd.DataFrame): Arrival destination summary (from `get_arrival_destinations`).

    Returns:
        pd.DataFrame: Unified destination table with combined stats.
    """
    destinations = list(set(departures['dep_airport_icao'].unique()).union(set(arrivals['arr_airport_icao'].unique())))
    airports_codes = pd.DataFrame(destinations, columns=['airport_icao'])  

    dep_destinations  = pd.merge(airports_codes, departures, how='left', left_on='airport_icao', right_on='dep_airport_icao')
    destinations = pd.merge(dep_destinations, arrivals, how='left', left_on='airport_icao', right_on='arr_airport_icao')

    destinations['airport_iata'] = np.where(destinations['dep_airport_iata'].isna(), destinations['arr_airport_iata'], destinations['dep_airport_iata']) 
    destinations['airport_name'] = np.where(destinations['dep_airport_name'].isna(), destinations['arr_airport_name'], destinations['dep_airport_name']) 
    destinations['city'] = np.where(destinations['dep_city'].isna(), destinations['arr_city'], destinations['dep_city']) 
    destinations['route_distance_miles'] = np.where(destinations['dep_route_distance_miles'].isna(), destinations['arr_route_distance_miles'], destinations['dep_route_distance_miles']).round(2)
    destinations['route_distance_km'] = destinations['route_distance_miles'] * 1.60934

    destinations['ete_duration_min'] = np.mean([destinations['dep_ete_duration_min'], destinations['arr_ete_duration_min']], axis=0)
    destinations['ete_duration_min'] = np.where(destinations['ete_duration_min'].isna(), destinations['dep_ete_duration_min'], destinations['ete_duration_min'])
    destinations['ete_duration_min'] = np.where(destinations['dep_ete_duration_min'].isna(), destinations['arr_ete_duration_min'], destinations['ete_duration_min']).round(2)

    destinations['ete_duration_hh_mm'] = destinations['ete_duration_min'].apply(decimal_minutes_to_hhmm)
    destinations['num_flights'] = destinations['num_departures'].fillna(0) + destinations['num_arrivals'].fillna(0)

    destinations = destinations[['airport_icao', 'airport_iata', 'airport_name', 'city','route_distance_miles', 'ete_duration_min', 'ete_duration_hh_mm','num_departures', 'num_arrivals', 'num_flights']]
    destinations = destinations.sort_values(by=['num_flights'], ascending=False).reset_index(drop=True)

    return destinations


def get_shortest_longest_route(destinations):
    """
    Identifies the destinations with the shortest and longest average route distances.

    Args:
        destinations (pd.DataFrame): DataFrame from `get_all_destinations()`.

    Returns:
        dict: Dictionary containing 'shortest' and 'longest' route details.
    """
    shortest_distance_route = destinations.sort_values(by='route_distance_miles').iloc[0]
    longest_distance_route = destinations.sort_values(by='route_distance_miles', ascending=False).iloc[0]
    
    shortest_longest_routes = {
        "shortest": shortest_distance_route.to_dict(),
        "longest": longest_distance_route.to_dict()
    }

    return shortest_longest_routes
