import sys
from datetime import date

from jobs.spark_convert_mat_csv import Bleja
from jobs.spark_create_data import CreateData

if __name__ == "__main__":
    """
    Usage: extreme-weather [year]
    Displays extreme weather stats (highest temperature, wind, precipitation) for the given, or latest, year.
    """

    extreme_weather = ExtremeWeather(year)
    extreme_weather.run()
