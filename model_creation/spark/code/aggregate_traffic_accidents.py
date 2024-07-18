from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, udf, when
from pyspark.sql.types import StringType

weather_params = ["Temperature(F)", "Humidity(%)", 
           "Pressure(in)", 
           "Wind_Speed(mph)", 
           "Weather_Condition"]

road_params = ["Amenity", "Crossing", 
           "Give_Way", "Junction", "No_Exit", "Railway", 
           "Stop", "Traffic_Calming", "Traffic_Signal"]

road_id = ["Street", "City", "County", "State"]

#https://openweathermap.org/weather-conditions
weather_mapping = {
    'clear sky': ['Clear', 'Fair', 'Fair / Windy'],
    'few clouds': ['Partly Cloudy', 'Partly Cloudy / Windy'],
    'scattered clouds': ['Scattered Clouds', 'Mostly Cloudy', 'Mostly Cloudy / Windy'],
    'broken clouds': ['Cloudy', 'Cloudy / Windy', 'Overcast', 'Squalls / Windy'],
    'shower rain': ['Light Rain Showers', 'Rain Showers', 'Rain Shower', 'Heavy Rain Showers', 
                    'Light Rain Shower', 'Rain Shower / Windy', 'Heavy Rain Shower / Windy'
                    ,'Squalls', 'Rain and Sleet', 'Light Rain Shower / Windy', 'Showers in the Vicinity', 
                    'Heavy Rain Shower', 'Light Hail', 'Small Hail', 'Hail'],
      
    
    'rain': ['Light Freezing Rain', 'Heavy Rain', 'Rain', 'Rain / Windy', 'Light Rain', 
             'Light Rain with Thunder', 'Heavy Rain / Windy', 'Thunderstorms and Rain', 
             'Light Rain / Windy', 'Freezing Rain', 'Light Freezing Rain / Windy', 
             'Heavy Freezing Rain', 'Heavy Freezing Rain / Windy', 'Freezing Rain / Windy',
             'Light Drizzle', 'Drizzle', 'Rain and Sleet', 'Drizzle and Fog', 'Drizzle / Windy', 
             'Light Drizzle / Windy', 'Heavy Drizzle'],
    
    
    'thunderstorm': ['Thunderstorm', 'Heavy Thunderstorms and Snow', 'Heavy Thunderstorms and Rain', 
                     'Heavy Thunderstorms with Small Hail', 'Light Thunderstorms and Rain', 
                     'Light Thunderstorms and Snow', 'Thunderstorms and Snow', 
                     'Light Thunderstorm', 'Thunder', 'Thunder in the Vicinity', 'Thunder / Windy', 
                     'Thunder / Wintry Mix', 'Thunder / Wintry Mix / Windy', 'Thunder and Hail', 
                     'Thunder and Hail / Windy', 'Heavy T-Storm / Windy', 'T-Storm', 
                     'T-Storm / Windy', 'Funnel Cloud', 'Sleet and Thunder', 'Tornado', 'Heavy T-Storm'],
    
    'snow': ['Blowing Snow', 'Heavy Snow', 'Snow', 'Snow Showers', 'Light Snow', 'Light Snow / Windy', 
             'Snow / Windy', 'Heavy Snow / Windy', 'Light Snow Shower', 'Light Snow Showers', 
             'Heavy Snow with Thunder', 'Light Snow with Thunder', 'Snow and Thunder', 
             'Snow and Thunder / Windy', 'Light Snow Grains', 'Drifting Snow', 'Drifting Snow / Windy',
             'Light Freezing Drizzle', 'Snow Grains', 'Heavy Ice Pellets', 'Blowing Snow / Windy', 
             'Heavy Blowing Snow', 'Light Snow Shower / Windy', 'Freezing Drizzle', 
             'Blowing Snow Nearby', 'Wintry Mix', 'Light Snow and Sleet / Windy', 
             'Low Drifting Snow', 'Snow and Sleet / Windy', 'Heavy Freezing Drizzle', 
             'Wintry Mix / Windy', 'Light Snow and Sleet', 'Heavy Sleet and Thunder', 
             'Light Blowing Snow', 'Light Snow Shower / Windy', 'Light Sleet / Windy', 
             'Light Ice Pellets', 'Ice Pellets', 'Light Sleet', 'Sleet', 'Heavy Sleet', 
             'Snow and Sleet', 'Sleet / Windy', 'Light Snow and Sleet / Windy', 'Heavy Sleet / Windy'],
    
    'mist': ['Mist', 'Light Haze', 'Haze', 'Light Freezing Fog', 'Fog', 'Shallow Fog', 'Fog / Windy', 
             'Haze / Windy', 'Partial Fog', 'Partial Fog / Windy', 'Mist / Windy', 
             'Sand / Dust Whirls Nearby', 'Duststorm', 'Blowing Dust / Windy', 
             'Shallow Fog / Windy', 'Smoke / Windy', 'Patches of Fog / Windy', 
             'Blowing Sand', 'Heavy Smoke', 'Dust Whirls', 'Smoke', 'Volcanic Ash', 
             'Sand / Dust Whirlwinds / Windy', 'N/A Precipitation',
             'Patches of Fog', 'Sand', 'Sand / Dust Whirlwinds', 'Widespread Dust / Windy', 
             'Widespread Dust', 'Sand / Windy', 'Light Fog', 'Blowing Dust']

}

def map_weather_udf(condition):
    for key, values in weather_mapping.items():
        if condition in values:
            return key
    return 'Unknown'


spark = SparkSession.builder.appName("AggregateTrafficAccidents").getOrCreate()

#set log as error
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv('/opt/tap/project_dataset/US_Accidents_March23.csv', header=True, inferSchema=True)

for param in weather_params + road_params + ["Sunrise_Sunset"]:
    df = df.filter(col(param).isNotNull())

map_weather = udf(map_weather_udf, StringType())
df = df.withColumn("Weather_Condition", map_weather(df['Weather_Condition']))

df= df.withColumn("Temperature(C)", ( (col("Temperature(F)")-32)*(5/9)) ) 

index = weather_params.index("Temperature(F)")
weather_params[index] = "Temperature(C)"

#rounding to integer values
for param in weather_params[:-1]:
    df = df.withColumn(param, round(col(param)).cast("int"))

for param in road_params:
    df = df.withColumn(param, when(col(param) == False, 0).when(col(param) == True, 1))


df_B = spark.read.csv('/opt/tap/input_data/final.csv', header=True, inferSchema=True)

df_B = df_B.withColumnRenamed("input_street", "Street").withColumnRenamed("input_city", "City"). \
    withColumnRenamed("input_county", "County").withColumnRenamed("input_state", "State")

df_join = df_B.join(df, on=road_id, how="inner")

grouped_df = df_join.groupBy(weather_params + road_params + ["Sunrise_Sunset"] + ["highway"]).count()

data = grouped_df.orderBy(col("count").desc())

single_partition_df = data.coalesce(1)

#save the result in a csv file
single_partition_df.write.csv("/opt/tap/output_data/Aggregate_t_a", header=True, mode='overwrite')

spark.stop()

