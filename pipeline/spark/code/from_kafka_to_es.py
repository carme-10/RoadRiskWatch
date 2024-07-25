from pyspark.sql.session import SparkSession
from pyspark.sql.functions import from_json
import pyspark.sql.types as tp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, substring, when, struct
from pyspark.ml.pipeline import PipelineModel
from pyspark.conf import SparkConf

import requests, json, time

def es_mapping(es_index):

    #URL of the Elasticsearch index
    url = f"http://elasticsearch:9200/{es_index}"

    #mapping data in JSON format
    mapping = {
        "mappings": {
            "properties": {
                "weather_timestamp_iso8601": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis"
                },
                "location": { 
                    "type": "geo_point"
                }
            }
        }
    }

    #try to make a request until es is available
    while True:
        time.sleep(1)
        try:
            #Send a PUT request to configure the mapping of the index
            response = requests.put(url, headers={"Content-Type": "application/json"}, data=json.dumps(mapping))
            response.raise_for_status()
            
            print("Elastic Search, mapping response:", response.text)
            break

        except requests.exceptions.RequestException as e:
            print(f'Waiting for Elastic Search: {e}')

def is_mapping_present(es_index):
    
    url = f"http://elasticsearch:9200/{es_index}/_mapping"

    while True:
        time.sleep(1)
        try:
            # Send a GET request to get the mapping of the index
            response = requests.get(url)
            response.raise_for_status()

            mapping_data = response.json()
            if mapping_data:
                print(f"Mapping for index '{es_index}' found")
                return True
            else:
                print(f"No mapping found for index '{es_index}'")
                return False

        except requests.exceptions.RequestException as e:

            if e is None:
                print(f'Waiting for Elastic Search')
                continue
            
            #In case the index does not exist, the response will be 404
            if e.response.status_code == 404:
                print(f"The index '{es_index}' does not exist")
                return False
            else:
                print(f'Waiting for Elastic Search: {e}')


kafkaServer="broker:9092"
topic = "roads-topic"

elastic_index="road_data"

model_version = "2024-07-25_15-44-05_GMT"

if not is_mapping_present(elastic_index):
    es_mapping(elastic_index)

print("Starting the Application")

sparkConf = SparkConf().set("es.nodes", "elasticsearch") \
                        .set("es.port", "9200")

spark = SparkSession.builder.appName("ReadFromKafkaAndWriteToES").config(conf=sparkConf).getOrCreate()

spark.sparkContext.setLogLevel("ERROR") 

print("Loading model")
model = PipelineModel.load(f"/opt/tap/models/{model_version}/model/")
print("Done")

#user defined function to map the weather icon to a more readable string
def map_icon(icon):
    if icon == "01d" or icon == "01n":
        return "clear sky"
    if icon == "02d" or icon == "02n":
        return "few clouds"
    if icon == "03d" or icon == "03n":
        return "scattered clouds"
    if icon == "04d" or icon == "04n":
        return "broken clouds"
    if icon == "09d" or icon == "09n":
        return "shower rain"
    if icon == "10d" or icon == "10n":
        return "rain"
    if icon == "11d" or icon == "11n":
        return "thunderstorm"
    if icon == "13d" or icon == "13n":
        return "snow"
    if icon == "50d" or icon == "50n":
        return "mist"
    return "unknown"

columns_struct = tp.StructType([
    tp.StructField(name= 'way_id', dataType= tp.LongType(),  nullable= True),
    tp.StructField(name= 'highway', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'name',       dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'Temperature_C', dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'Humidity_%', dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'pressure_hPa', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'wind_speed_meterSec', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'weather_icon', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= "Give_Way", dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= "Crossing", dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= "Traffic_Calming", dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= "Traffic_Signal", dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= "Railway", dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= "Stop", dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= "Amenity", dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= "No_Exit", dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= "Junction", dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= "lat", dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= "lon", dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= "weather_timestamp_iso8601", dataType= tp.StringType(),  nullable= True)
])


# Read the stream from kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()

data_columns_str = ["data."+field.name for field in columns_struct.fields]

# Cast the message received from kafka with the provided schema
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", columns_struct).alias("data")) \
    .select(data_columns_str)

#rename columns
df= df.withColumnRenamed("Temperature_C", "Temperature(C)")
df= df.withColumnRenamed("Humidity_%", "Humidity(%)")

df= df.withColumn("Pressure(in)", (col("pressure_hPa")*0.02953).cast(tp.IntegerType()))
df= df.withColumn("Wind_Speed(mph)", (col("wind_speed_meterSec")*2.23694).cast(tp.IntegerType()))

#create a new column with the last char of the weather_icon as value (d or n),
#spark string index starts from 1, that's why the 3
df = df.withColumn("Sunrise_Sunset", substring(df["weather_icon"], 3, 1))
#convert the d/n in Day/Night
df = df.withColumn("Sunrise_Sunset", when(df["Sunrise_Sunset"] == "d", "Day").otherwise("Night"))

map_icon_udf = udf(map_icon)
df = df.withColumn("Weather_Condition", map_icon_udf(col("weather_icon")))

df = df.drop("pressure_hPa", "wind_speed_meterSec", "weather_icon")

# # Apply the machine learning model
df=model.transform(df)

#location column with lat and lon
df = df.withColumn("location", struct(col("lat"), col("lon")))

df = df.select("way_id", "highway", "name", "Temperature(C)", "Humidity(%)", "Pressure(in)", \
               "Wind_Speed(mph)", "Sunrise_Sunset", "Weather_Condition", "Amenity", "Give_Way", \
                   "Crossing", "Traffic_Calming", "Traffic_Signal", "Railway", "Stop", "No_Exit", \
                    "Junction", "location", "weather_timestamp_iso8601", "prediction")

df = df.withColumnRenamed("prediction", "danger_index")

# df.writeStream \
#    .format("console") \
#    .option("truncate",True) \
#    .start() \
#    .awaitTermination()

df.writeStream \
   .option("checkpointLocation", "/tmp/") \
   .format("es") \
   .start(elastic_index) \
   .awaitTermination()
