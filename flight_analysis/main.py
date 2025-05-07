"""
Flight Data Reporting Script

Processes flight data for a given airport, calculates KPIs, generates a natural language summary,
and saves outputs as JSON, CSV, and PDF reports.
"""

import os
import sys
import json
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import markdown
from dotenv import load_dotenv
from weasyprint import HTML
from openai import OpenAI
from generate_report import generate_report, convert_to_pdf, load_prompts, plot_flights_per_hour_bar_chart
from preprocessing import list_files, filter_flights, load_flight_data
from general_kpis import (
    get_general_metrics,
    get_arrival_delay_metrics,
    get_departure_delay_metrics,
    get_flights_per_hour,
    identify_peaks
)
from infrastructure_metrics import *
from airline_metrics import *
from destination_metrics import *


def generate_metrics(airport: str, arrivals: pd.DataFrame, departures: pd.DataFrame) -> dict:
    """Generate structured airport KPIs and metrics."""
    combined = pd.concat([departures, arrivals])
    flights_hourly = get_flights_per_hour(arrivals, departures)
    flights_with_peaks = (
        flights_hourly
        .pipe(identify_peaks, "arrivals")
        .pipe(identify_peaks, "departures")
    )
    destinations = get_all_destinations(
        get_departure_destinations(departures),
        get_arrival_destinations(arrivals)
    )
    routes = get_shortest_longest_route(destinations)

    return {
        "airport": airport.upper(),
        "general_metrics": {
            "arrivals": get_general_metrics(arrivals),
            "departures": get_general_metrics(departures),
            "arrival_delays": get_arrival_delay_metrics(arrivals),
            "departure_delays": get_departure_delay_metrics(departures),
            "flights_per_hour": flights_hourly.to_dict(),
            "flights_per_hour_with_peaks": flights_with_peaks.to_dict()
        },
        "infrastructure_metrics": {
            "terminals": {
                "departures_per_terminal": get_departures_per_terminal(departures).to_dict(),
                "departures_delays_per_terminal": get_terminal_departure_delays(departures).to_dict(),
                "arrivals_per_terminal": get_arrivals_per_terminal(arrivals).to_dict(),
                "arrivals_delays_per_terminal": get_terminal_arrival_delays(arrivals).to_dict(),
            },
            "gates": {
                "departures_per_gate": get_departures_per_gate(departures).head(5).to_dict(),
                "arrivals_per_gate": get_arrivals_per_gate(arrivals).head(5).to_dict(),
            },
            "runways": {
                "departures_per_runway": get_departures_per_runway(departures).to_dict(),
                "arrivals_per_runway": get_arrivals_per_runway(arrivals).to_dict(),
            }
        },
        "airline_metrics": {
            "arrivals_kpis": get_arrivals_kpis_per_airline(arrivals).head(5).to_dict(),
            "arrival_routes": get_arrivals_routes_per_airline(arrivals).head(5).to_dict(),
            "departures_kpis": get_departures_kpis_per_airline(departures).head(5).to_dict(),
            "departure_routes": get_departure_routes_per_airline(departures).head(5).to_dict(),
            "aircrafts": get_aircrafts_per_airline(combined).head(5).to_dict(),
            "net_delays": (
                add_inbound_with_outbound_flights(arrivals, departures)
                .pipe(calculate_net_delays)
                .pipe(calculate_net_delays_per_airline)
                .to_dict()
            )
        },
        "destination_metrics": {
            "total_destinations": len(destinations),
            "departure_destinations": len(get_departure_destinations(departures)),
            "arrival_destinations": len(get_arrival_destinations(arrivals)),
            "top_10_destinations": destinations.head().to_dict(),
            "shortest_route": routes["shortest"],
            "longest_route": routes["longest"]
        }
    }


def safe_json(obj):
    if isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        return float(obj)
    elif isinstance(obj, (np.bool_,)):
        return bool(obj)
    elif isinstance(obj, (pd.Timestamp, pd.Timedelta)):
        return str(obj)
    elif isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    return str(obj)


def main() -> None:
    """Main execution pipeline for flight data ingestion."""
    if len(sys.argv) != 2:
        print("Usage: python flights_ingestion.py <airport_code>")
        sys.exit(1)

    load_dotenv()
    airport = sys.argv[1].lower()
    api_key = os.getenv("OPENAI_API_KEY")

    system_prompt, user_prompt_template = load_prompts("system_prompt.txt", "user_prompt.txt")
    departures, arrivals = load_flight_data(airport)
    metrics = generate_metrics(airport, arrivals, departures)

    plot_flights_per_hour_bar_chart(metrics)
    print("✅ Generated flight distribution graph")


    try:
        json.dumps(metrics, default=safe_json)
    except TypeError as e:
        print("Serialization error:", e)

    with open(f"{airport}_metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=4, default=safe_json)

    user_prompt = f"{user_prompt_template}\n\n{metrics}"

    report_md = generate_report(api_key, system_prompt, user_prompt)
    convert_to_pdf(report_md, airport, f"airport_report_{airport}.pdf")

    destinations = get_all_destinations(
         get_departure_destinations(departures),
         get_arrival_destinations(arrivals)
     )
    pd.DataFrame(destinations).to_csv(f"{airport}_destinations.csv", index=False)

    print(f"✅ Metrics and report for {airport.upper()} generated.")


if __name__ == "__main__":
    main()
