import os

from openweathermap import OpenWeatherMapLakeflowConnect


api_key = os.environ["OPENWEATHER_API_KEY"]

connector = OpenWeatherMapLakeflowConnect(
    {
        "api_key": api_key,
    }
)

records, offset = connector.read_table(
    "current_weather",
    {},
   {
    "place_name": "Pune",
},
)

record = next(records)

print("City:", record["name"])
print("Coordinates:", record["coord"])
print("Temperature:", record["main"]["temp"])
print("Weather:", record["weather"])
print("Offset:", offset)