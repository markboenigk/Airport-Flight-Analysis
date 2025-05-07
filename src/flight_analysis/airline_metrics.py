import pandas as pd 
import numpy as np
from datetime import datetime, timedelta


# Airlines 
def get_departures_kpis_per_airline(departures: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates average departure delay and route distance per airline.

    Parameters:
        departures (pd.DataFrame): Departure flights data.

    Returns:
        pd.DataFrame: KPI summary per airline including number of departures, average delay, and route distance.
    """

    departures_per_airline = departures.groupby('operator').agg({'fa_flight_id': 'count', 'departure_delay':'sum', 'route_distance':'mean'}).reset_index()
    departures_per_airline['avg_departure_delay_min'] = round(departures_per_airline['departure_delay'] / departures_per_airline['fa_flight_id'] / 60,2)
    departures_per_airline = departures_per_airline[['operator', 'fa_flight_id', 'avg_departure_delay_min', 'route_distance']].sort_values(by='fa_flight_id', ascending=False).reset_index(drop=True)
    departures_per_airline.columns = ['airline', 'num_departures', 'avg_departure_delay_min', 'avg_route_distance_miles']
    departures_per_airline['avg_route_distance_km'] = round(departures_per_airline['avg_route_distance_miles'] * 1.609344,2)
    departures_per_airline['avg_route_distance_miles'] = departures_per_airline['avg_route_distance_miles'].round(2)
    departures_per_airline = departures_per_airline.sort_values(by='num_departures', ascending=False).reset_index(drop=True)
    return departures_per_airline

def get_departure_routes_per_airline(departures: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates number of departures, average delay, and route distance per airline and destination.

    Parameters:
        departures (pd.DataFrame): Departure flights data.

    Returns:
        pd.DataFrame: KPI summary per airline and destination.
    """
    departure_routes_per_airline = departures.groupby(['operator','destination_code' ]).agg({'fa_flight_id': 'count', 'departure_delay':'sum','route_distance':'mean'}).reset_index()
    departure_routes_per_airline['avg_departure_delay_min'] = round(departure_routes_per_airline['departure_delay'] / departure_routes_per_airline['fa_flight_id'] / 60,2)
    departure_routes_per_airline = departure_routes_per_airline[['operator', 'destination_code','fa_flight_id', 'avg_departure_delay_min', 'route_distance']].sort_values(by='fa_flight_id', ascending=False).reset_index(drop=True)
    departure_routes_per_airline.columns = ['airline', 'destination_code','num_departures', 'avg_departure_delay_min', 'route_distance_miles']
    departure_routes_per_airline['route_distance_km'] = round(departure_routes_per_airline['route_distance_miles'] * 1.609344,2)
    departure_routes_per_airline['route_distance_miles'] = departure_routes_per_airline['route_distance_miles'].round(2)
    departure_routes_per_airline = departure_routes_per_airline.sort_values(by='num_departures', ascending=False).reset_index(drop=True)
    return departure_routes_per_airline

def get_arrivals_kpis_per_airline(arrivals: pd.DataFrame) -> pd.DataFrame:
    """
    Computes average arrival delay and route distance per airline.

    Parameters:
        arrivals (pd.DataFrame): Arrival flights data.

    Returns:
        pd.DataFrame: KPI summary per airline including number of arrivals, average delay, and route distance.
    """
    arrivals_per_airline = arrivals.groupby('operator').agg({'fa_flight_id': 'count', 'arrival_delay':'sum', 'route_distance':'mean'}).reset_index()
    arrivals_per_airline['avg_arrival_delay_min'] = round(arrivals_per_airline['arrival_delay'] / arrivals_per_airline['fa_flight_id'] / 60,2)
    arrivals_per_airline = arrivals_per_airline[['operator', 'fa_flight_id', 'avg_arrival_delay_min', 'route_distance']].sort_values(by='fa_flight_id', ascending=False).reset_index(drop=True)
    arrivals_per_airline.columns = ['airline', 'num_arrivals', 'avg_arrival_delay_min', 'avg_route_distance_miles']
    arrivals_per_airline['avg_route_distance_km'] = round(arrivals_per_airline['avg_route_distance_miles'] * 1.609344,2)
    arrivals_per_airline['avg_route_distance_miles'] = arrivals_per_airline['avg_route_distance_miles'].round(2)
    arrivals_per_airline = arrivals_per_airline.sort_values(by='num_arrivals', ascending=False).reset_index(drop=True)
    return arrivals_per_airline

def get_arrivals_routes_per_airline(arrivals: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates arrival delay and route metrics per airline and origin airport.

    Parameters:
        arrivals (pd.DataFrame): Arrival flights data.

    Returns:
        pd.DataFrame: KPI summary per airline and origin airport.
    """
    arrival_routes_per_airline = arrivals.groupby(['operator','destination_code' ]).agg({'fa_flight_id': 'count', 'arrival_delay':'sum','route_distance':'mean'}).reset_index()
    arrival_routes_per_airline['avg_arrival_delay_min'] = round(arrival_routes_per_airline['arrival_delay'] / arrival_routes_per_airline['fa_flight_id'] / 60,2)
    arrival_routes_per_airline = arrival_routes_per_airline[['operator', 'destination_code','fa_flight_id', 'avg_arrival_delay_min', 'route_distance']].sort_values(by='fa_flight_id', ascending=False).reset_index(drop=True)
    arrival_routes_per_airline.columns = ['airline', 'origin_code','num_arrivals', 'avg_arrival_delay_min', 'route_distance_miles']
    arrival_routes_per_airline['route_distance_km'] = round(arrival_routes_per_airline['route_distance_miles'] * 1.609344,2)
    arrival_routes_per_airline['route_distance_miles'] = arrival_routes_per_airline['route_distance_miles'].round(2)
    arrival_routes_per_airline = arrival_routes_per_airline.sort_values(by='num_arrivals', ascending=False).reset_index(drop=True)
    return arrival_routes_per_airline

def get_aircrafts_per_airline(flights: pd.DataFrame) -> pd.DataFrame:
    """
    Counts unique aircraft types per airline based on number of flights.

    Parameters:
        flights (pd.DataFrame): Flight data with aircraft type and airline.

    Returns:
        pd.DataFrame: Number of flights per aircraft type per airline.
    """
    aircrafts_per_airline = flights.groupby(['operator', 'aircraft_type'])['fa_flight_id'].nunique().reset_index()
    aircrafts_per_airline.columns = ['airline', 'aircraft_type', 'num_flights']
    aircrafts_per_airline = aircrafts_per_airline.sort_values(by='num_flights', ascending=False).reset_index(drop=True)
    return aircrafts_per_airline

def add_inbound_with_outbound_flights(arrivals: pd.DataFrame, departures: pd.DataFrame) -> pd.DataFrame:
    """
    Merges inbound arrival flights with their associated outbound departures.

    Parameters:
        arrivals (pd.DataFrame): Arrival flight data.
        departures (pd.DataFrame): Departure flight data.

    Returns:
        pd.DataFrame: Combined dataset of inbound and outbound flights.
    """
    arrivals = arrivals[['fa_flight_id','ident_icao','ident_iata', 'origin_code', 'destination_code', 'scheduled_in_utc','estimated_in_utc','actual_in_utc', 'arrival_delay', 'terminal_destination', 'gate_destination']]
    arrivals = arrivals.rename(columns={'fa_flight_id':'arr_fa_flight_id','ident_icao':'arr_ident_icao','ident_iata':'arr_ident_iata','origin_code': 'arr_origin_code', 'destination_code':'arr_destination_code'})
    departures = departures[['inbound_fa_flight_id','fa_flight_id','operator','ident_icao','ident_iata', 'origin_code', 'destination_code', 'scheduled_out_utc','estimated_out_utc','actual_out_utc', 'departure_delay', 'terminal_origin', 'gate_origin']]
    departures = departures.rename(columns={'fa_flight_id':'dep_fa_flight_id','ident_icao':'dep_ident_icao','ident_iata':'dep_ident_iata','origin_code': 'dep_origin_code', 'destination_code':'dep_destination_code'})
    in_outbound_flights = arrivals.merge(departures, left_on='arr_fa_flight_id', right_on='inbound_fa_flight_id', how='inner')
    return in_outbound_flights

def calculate_net_delays(in_outbound_flights: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the net delay between outbound and inbound flights.

    Parameters:
        in_outbound_flights (pd.DataFrame): Merged arrival-departure records.

    Returns:
        pd.DataFrame: Updated with delay columns in minutes and net delay.
    """
    in_outbound_flights['net_delay'] = in_outbound_flights['departure_delay'] - in_outbound_flights['arrival_delay']
    for col in ['arrival_delay','departure_delay', 'net_delay']:
        in_outbound_flights[col + '_min'] = round(in_outbound_flights[col] / 60, 2)
    return in_outbound_flights

def calculate_net_delays_per_airline(in_outbound_flights: pd.DataFrame) -> pd.DataFrame:
    """
    Computes median net delay per airline from linked inbound-outbound flights.

    Parameters:
        in_outbound_flights (pd.DataFrame): Flights with calculated net delay.

    Returns:
        pd.DataFrame: Median net delay and total flights per airline.
    """
    net_delays_airline = in_outbound_flights.groupby('operator').agg({'net_delay_min': 'median', 'dep_fa_flight_id':'count'}).sort_values(by='net_delay_min', ascending=False).reset_index()
    net_delays_airline = net_delays_airline.rename(columns={'dep_fa_flight_id':'num_flights', 'net_delay_min':'median_net_flight_delay_min'})
    return net_delays_airline