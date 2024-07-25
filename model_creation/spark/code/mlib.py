from pyspark.sql import SparkSession

from pyspark.sql.functions import col, when, sum, count, abs, greatest, least, avg, log, sqrt, min, max

from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.ml import Pipeline

import time

spark = SparkSession.builder \
    .appName("RandomForestRegressionModel") \
    .getOrCreate()

df = spark.read.csv("/opt/tap/input_data/FinalJoin_transformed.csv", header=True, inferSchema=True)

predictor_columns = ["Temperature(C)", "Humidity(%)", 
           "Pressure(in)", 
           "Wind_Speed(mph)", 
           "Amenity", "Crossing", 
           "Give_Way", "Junction", "No_Exit", "Railway", 
           "Stop", "Traffic_Calming", "Traffic_Signal", 
           "highway_indexed", "Weather_Condition_indexed", "Sunrise_Sunset_indexed"] 

predictors_column_name = "predictors" 
label_column_name = "row_count_norm" 
predizione = "prediction" #the model prediction will be stored in this column

indexer = StringIndexer(inputCols=["highway", "Weather_Condition", "Sunrise_Sunset"], \
                        outputCols=["highway_indexed", "Weather_Condition_indexed", "Sunrise_Sunset_indexed"])

assembler = VectorAssembler(inputCols=predictor_columns, outputCol=predictors_column_name)

rf = RandomForestRegressor(labelCol=label_column_name, featuresCol=predictors_column_name, \
                               predictionCol=predizione, numTrees=40, maxDepth=11, \
                                featureSubsetStrategy="all")

pipeline = Pipeline(stages=[indexer, assembler, rf])

# Fit model
model = pipeline.fit(df)

# df of predictions
predictions = model.transform(df)

# Evaluate model
evaluator = RegressionEvaluator(labelCol=label_column_name, predictionCol=predizione, metricName="rmse")
rmse = evaluator.evaluate(predictions)

timestamp = time.time()
utc_time = time.gmtime(timestamp)
readable_utc_time = time.strftime("%Y-%m-%d_%H-%M-%S_%Z", utc_time)

model_path = f"/opt/tap/models/{readable_utc_time}/"

print("Saving model...")
model.write().overwrite().save(model_path + "model/")
print("Done.")

# Extract the RandomForestRegressor model from the pipeline
rf_model = model.stages[-1]

feature_importances = rf_model.featureImportances

min_pred = predictions.select(min(predizione)).first()[0]
max_pred = predictions.select(max(predizione)).first()[0]

with open(model_path + "model_info.txt", 'a') as file:
    
    file.write("min_pred: " + str(min_pred) + " max_pred: " + str(max_pred) + " rmse:" + str(rmse) + "\n")
    file.write("Feature importances:" + "\n")

    for i, feature in enumerate(predictor_columns):
        file.write(f"{feature}: {feature_importances[i]}" + "\n")
    file.write("-------------------"+ "\n")

spark.stop()
