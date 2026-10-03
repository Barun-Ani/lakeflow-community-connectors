import requests
from typing import Iterator
from databricks.labs.community_connector.interface import LakeflowConnect
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    IntegerType,
)

class OpenWeatherMapLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]):
        super().__init__(options)

        self.api_key = options["api_key"]


    def list_tables(self) -> list[str]:
        return ["current_weather"]


    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        if table_name != "current_weather":
            raise ValueError(f"Unsupported table: {table_name}")

        return StructType(
            [
                StructField(
                    "coord",
                    StructType(
                        [
                            StructField("lon", DoubleType(), True),
                            StructField("lat", DoubleType(), True),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "weather",
                    ArrayType(
                        StructType(
                            [
                                StructField("id", IntegerType(), True),
                                StructField("main", StringType(), True),
                                StructField("description", StringType(), True),
                                StructField("icon", StringType(), True),
                            ]
                        )
                    ),
                    True,
                ),
                StructField(
                    "main",
                    StructType(
                        [
                            StructField("temp", DoubleType(), True),
                            StructField("feels_like", DoubleType(), True),
                            StructField("temp_min", DoubleType(), True),
                            StructField("temp_max", DoubleType(), True),
                            StructField("pressure", IntegerType(), True),
                            StructField("humidity", IntegerType(), True),
                            StructField("sea_level", IntegerType(), True),
                            StructField("grnd_level", IntegerType(), True),
                        ]
                    ),
                    True,
                ), 
                StructField(
                    "wind",
                    StructType(
                        [
                            StructField("speed", DoubleType(), True),
                            StructField("deg", IntegerType(), True),
                            StructField("gust", DoubleType(), True),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "clouds",
                    StructType(
                        [
                            StructField("all", IntegerType(), True),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "rain",
                    StructType(
                        [
                            StructField("1h", DoubleType(), True),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "snow",
                    StructType(
                        [
                            StructField("1h", DoubleType(), True),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "sys",
                    StructType(
                        [
                            StructField("type", IntegerType(), True),
                            StructField("id", IntegerType(), True),
                            StructField("country", StringType(), True),
                            StructField("sunrise", LongType(), True),
                            StructField("sunset", LongType(), True),
                        ]
                    ),
                    True,
                ),
                StructField("base", StringType(), True),
                StructField("visibility", IntegerType(), True),
                StructField("dt", LongType(), True),
                StructField("timezone", IntegerType(), True),
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("cod", IntegerType(), True),
            ]
        )


    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        if table_name != "current_weather":
            raise ValueError(f"Unsupported table: {table_name}")

        return {
            "primary_keys": ["id", "dt"],
            "cursor_field": "dt",
            "ingestion_type": "append",
        }


    def read_table(self,table_name: str,start_offset: dict,table_options: dict[str, str],) -> tuple[Iterator[dict], dict]:
        if table_name != "current_weather":
            raise ValueError(f"Unsupported table: {table_name}")

        place_name = table_options.get("place_name")
        latitude = table_options.get("latitude")
        longitude = table_options.get("longitude")

        if not place_name and not (latitude and longitude):
            raise ValueError(
                "Provide either 'place_name' or both 'latitude' and 'longitude'."
            )

        if latitude and longitude:
            lat = float(latitude)
            lon = float(longitude)
        else:
            geocode_response = requests.get(
                "https://api.openweathermap.org./geo/1.0/direct",
                params={
                    "q": place_name,
                    "limit": 1,
                    "appid": self.api_key,
                },
                timeout=30,
            )

            geocode_response.raise_for_status()

            locations = geocode_response.json()

            if not locations:
                raise ValueError(f"Location not found: {place_name}")

            lat = float(locations[0]["lat"])
            lon = float(locations[0]["lon"])
            
        response = requests.get(
            "https://api.openweathermap.org./data/2.5/weather",
            params={
                "lat": lat,
                "lon": lon,
                "appid": self.api_key,
                "units": "metric",
            },
            timeout=30,
        )

        response.raise_for_status()

        weather_data = response.json()

        return iter([weather_data]), {"cursor": weather_data["dt"]}