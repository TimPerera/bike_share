import requests
import pandas as pd

url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"
params = {
    "format": "csv",
    "stationID": 48549,  # Station ID for Toronto
    "Year": 2023,        # Specify the year
    "Month": 1,          # Specify the month
    "Day": 1,
    "timeframe": 2,      # Daily data
    "submit": "Download+Data"
}

response = requests.get(url, params=params)
with open("toronto_weather_data.csv", "wb") as file:
    file.write(response.content)

# Load the data into a pandas DataFrame
df = pd.read_csv("toronto_weather_data.csv")
print(df.head())
