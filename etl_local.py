import pandas as pd
import json
import os
import requests
import s3fs
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Función que convierte la temperatura de Kelvin a Fahrenheit.
def kelvin_to_fahrenheit(temp_in_kelvin):
    return (temp_in_kelvin - 273.15) * 9/5 + 32

# Función que transforma y carga los datos obtenidos.
def transform_load_data(data):
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]

    time_of_record = datetime.utcfromtimestamp(data['dt']) + timedelta(seconds=data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise']) + timedelta(seconds=data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset']) + timedelta(seconds=data['timezone'])

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (F)": temp_fahrenheit,
        "Feels Like (F)": feels_like_fahrenheit,
        "Min Temp (F)": min_temp_fahrenheit,
        "Max Temp (F)": max_temp_fahrenheit,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise (Local Time)": sunrise_time,
        "Sunset (Local Time)": sunset_time
    }

    df_data = pd.DataFrame([transformed_data])
    dt_string = datetime.utcnow().strftime("%d%m%Y%H%M%S")
    filename = f"current_weather_data_{city}_{dt_string}.csv"
    s3_path = f"s3://weather-data-engineering-project/{filename}"
    
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_token = os.getenv("AWS_SESSION_TOKEN")

    if not aws_key or not aws_secret:
        raise ValueError("AWS credentials are missing.")
    
    fs = s3fs.S3FileSystem(key=aws_key, secret=aws_secret, token=aws_token)
    with fs.open(s3_path, 'w') as f:
        df_data.to_csv(f, index=False)

def main():
    cityname = "Portland"
    API_KEY = os.getenv("WEATHER_API_KEY")
    if not API_KEY:
        raise ValueError("Weather API key is missing.")
    
    endpoint = f"http://api.openweathermap.org/data/2.5/weather?q={cityname}&APPID={API_KEY}"
    response = requests.get(endpoint)
    
    if response.status_code == 200:
        data = response.json()
        transform_load_data(data)
    else:
        print(f"Failed to get data: {response.status_code}, {response.text}")

if __name__ == "__main__":
    main()