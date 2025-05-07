import os 
import pandas as pd
import markdown
import plotly.io as pio
import markdown
import plotly.graph_objects as go
import plotly.io as pio
from typing import Tuple
from weasyprint import HTML
from openai import OpenAI
def generate_report(api_key: str, system_prompt: str, user_prompt: str) -> str:
    """Generate report summary text using OpenAI API."""
    client = OpenAI(api_key=api_key)
    response = client.responses.create(
        model="gpt-4o",
        instructions=system_prompt,
        input=user_prompt
    )
    return response.output_text.replace(
        "{INSERTFLIGHTDISTRIBUTION}",
        '<div class="image-container"><img src="flight_distribution.png" alt="Flight Distribution" style="width: 100%;"></div>'
    )


def convert_to_pdf(markdown_text: str, airport: str, output_path: str) -> None:
    """Convert markdown content to styled PDF using WeasyPrint."""
    html_body = markdown.markdown(markdown_text, extensions=["md_in_html", "tables"])

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>Airport Flight Report {airport.upper()}</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                padding: 40px;
                line-height: 1.6;
            }}
            h1, h2 {{
                color: #2a2a2a;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 8px;
            }}
            .image-container {{
                margin: 20px 0;
                text-align: center;
            }}
        </style>
    </head>
    <body>
        <h1>Airport Flight Report for {airport.upper()}</h1>
        {html_body}
    </body>
    </html>
    """
    HTML(string=html, base_url=os.getcwd()).write_pdf(output_path)

def plot_flights_per_hour_bar_chart(metrics: dict) -> None:
    """Plot and save a grouped bar chart of hourly arrivals and departures."""
    df = pd.DataFrame({
        "hour": list(metrics["general_metrics"]["flights_per_hour"]["scheduled_hour"].values()),
        "departures": list(metrics["general_metrics"]["flights_per_hour"]["num_departures"].values()),
        "arrivals": list(metrics["general_metrics"]["flights_per_hour"]["num_arrivals"].values())
    })

    df["hour"] = df["hour"].astype(int).apply(lambda h: f"{h:02d}:00")

    fig = go.Figure()
    fig.add_bar(x=df["hour"], y=df["departures"], name="Departures", marker_color="lightblue")
    fig.add_bar(x=df["hour"], y=df["arrivals"], name="Arrivals", marker_color="#0d3d69")

    fig.update_layout(
        barmode="group",
        title="Flights per Hour",
        xaxis_title="Hour",
        yaxis_title="Number of Flights",
        template="plotly_white",
        legend=dict(x=0.01, y=0.99)
    )
    pio.write_image(fig, "flight_distribution.png")

def load_prompts(system_path: str, user_path: str) -> Tuple[str, str]:
    """Load system and user prompts from text files."""
    with open(system_path, "r", encoding="utf-8") as sys_file:
        system_prompt = sys_file.read()
    with open(user_path, "r", encoding="utf-8") as user_file:
        user_prompt = user_file.read()
    return system_prompt, user_prompt