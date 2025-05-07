# Airport-Flight-Analysis

The Airport Flight Analysis repository is a Python-based repository analyzing flights of an airport. 
The flights analysis code is separated into two steps 1) flight ingestions, 2) flight analysis. As a end result the code generates a pdf report with an analysis of the flight data of the airport

**Tech Stack:** Python, Plotly (Visualization), Weasyprint (PDF generation), FlightAware Aero API, OpenAI API 


### Flight Ingestion
The script utilizes the Flightaware Aero API to fetch the historic arrivals and departures for an airport, transforms the flights and then stores the results in a local folder
The script is airport and date agnostic and can fetch historic flights up to the last ten days in the past. 
The script is executed in the following way:
```
python flights_ingestion.py <weekday> <airport_code>
```

### Flight Analysis

The flight analysis script is separated in one main.py script and mutiple other sub-scripts. The script processes the data from 1) and makes some analytical transformations before kpis and tables are calculated by the other sub scripts. The result is stored in in a metrics.json file for later processing. 

The script is executed 
```
python flights_ingestion.py <airport_code>
```

The subscripts are: 
- **preprocessing.py** - Loads the data and adds analytical dimensions 
- **general_kpis.py** - Calculates total flights, delay metrics
- **infrastructure_metrics.py** - Calculates metrics around terminals and gates, e.g. # departures per terminal, gate utilization
- **airline_metrics.py** - Calculates metrics around airlines, e.g. # departures per airline, delays per airline 
- **destination_metrics.py** - Calculates metrics around the destinations, e.g. arrival./ departure destinations, shortes/longest route 

The second part of the flight anaylysis uses OpenAI's API to generate a written pdf report about the flight data. 
- **system_prompt.txt** - System prompt for the OpenAI Response API
- **user_prompt.txt** - User prompt for the OpenAI Response API
- **generate_report.py** - Creates a pdf report based on the generated text and utilizes wesyprint for the html to pdf generation and Plotly to create a graphic

You can find an example report here: [Boston Logan Airport Analysis](https://github.com/markboenigk/Airport-Flight-Analysis/blob/main/airport_report_kbos.pdf)
