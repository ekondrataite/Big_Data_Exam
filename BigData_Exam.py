"""
# Environment
"""
from pyspark.sql.types import FloatType
import pandas as pd
import matplotlib.pyplot as plt
import folium
from datetime import datetime, timedelta
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession

import zipfile
import os
import pandas as pd
import tempfile

# Create a SparkSession
spark = SparkSession.builder.appName("Big Data Exam").master("local[*]").getOrCreate()

"""
# Task
"""

# Haversine distance function
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # Radius of Earth in kilometers
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    return R * c

# Register the Haversine function
spark.udf.register("haversine", haversine)

# Area
center_lat = 55.225000
center_lon = 14.245000
radius_km = 50

def process_day_file(file_path):

  df = spark.read.csv(file_path, header=True, inferSchema=True)

  # DATA PREPROCESSING:

  # Ensure that the data types for latitude, longitude, and timestamp are appropriate for calculations and sorting.
  df_new = df.withColumn("# Timestamp", F.to_timestamp(F.col("# Timestamp"), "dd/MM/yyyy HH:mm:ss"))

  df_new = df_new.select(
      F.col("# Timestamp").alias("timestamp"),
      F.col("MMSI").cast("string"),
      F.col("Latitude").cast("double").alias("latitude"),
      F.col("Longitude").cast("double").alias("longitude"),
      F.col("Name").alias("name")
  ).dropDuplicates()

  # Ensure logical values fo Latitude and Longitude
  df_new = df_new.filter(
    (F.col("latitude") >= -90) & (F.col("latitude") <= 90) & (F.col("longitude") >= -180) & (F.col("longitude") <= 180)
    )

  # PROCESS THE DATA: find the clossest vessels

  # Calculate distance from center for each vessel
  df_new = df_new.withColumn("distance_from_center",
                            haversine(F.col("latitude"), F.col("longitude"), F.lit(center_lat), F.lit(center_lon)))

  # Filter by distance from center to get vessels in the desired circle
  df_new = df_new.filter(F.col("distance_from_center") <= radius_km)

  # Self-join to find the closest vessels in the dataframe
  df1 = df_new.select(*(F.col(x).alias(x + "_df1") for x in df_new.columns))
  df2 = df_new.select(*(F.col(x).alias(x + "_df2") for x in df_new.columns))
  joined_df = df1.join(df2,
                       (F.col("timestamp_df1") == F.col("timestamp_df2")) & (F.col("MMSI_df1") != F.col("MMSI_df2")),
                       "inner"
                       )

  # Find the distances between vessels
  joined_df = joined_df.withColumn("distance",
                                 haversine(F.col("latitude_df1"), F.col("longitude_df1"), F.col("latitude_df2"), F.col("longitude_df2")))

  # Find the closest pair of vessels
  closest_vessels_first = joined_df.orderBy(F.col("distance")).first()

  mmsi_a = closest_vessels_first["MMSI_df1"]
  mmsi_b = closest_vessels_first["MMSI_df2"]
  rendezvous_time = closest_vessels_first["timestamp_df1"]

  # +/- 10 minutes
  after_rendezvous_obj = rendezvous_time + timedelta(minutes=10)
  before_rendezvous_obj = rendezvous_time - timedelta(minutes=10)

  # Convert back to string
  after_rendezvous = after_rendezvous_obj.strftime("%d/%m/%Y %H:%M:%S")
  before_rendezvous = before_rendezvous_obj.strftime("%d/%m/%Y %H:%M:%S")

  # Get each ship trajectories
  trajectory_a = df_new.where((F.col("MMSI") == mmsi_a) & F.col("timestamp").between(before_rendezvous,  after_rendezvous))
  trajectory_b = df_new.where((F.col("MMSI") == mmsi_b) & F.col("timestamp").between(before_rendezvous,  after_rendezvous))

  df_result = trajectory_a.unionByName(trajectory_b)

  return df_result

zip_path = "aisdk-2021-12.zip"

# Create a temporary directory to store extracted csv files
temp_dir = tempfile.mkdtemp()

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
  zip_ref.extractall(temp_dir)

csv_files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith('.csv')]

result_df = None

for file in csv_files:
    df = spark.read.csv(file, header=True, inferSchema=True)

    # Process day file
    processed_df = process_day_file(df)

    # Collect the results
    if result_df is None:
        result_df = processed_df
    else:
        result_df = result_df.union(processed_df)

# After gathering each day results we repeat some of the steps from the function:

df1 = result_df.select(*(F.col(x).alias(x + '_df1') for x in result_df.columns))
df2 = result_df.select(*(F.col(x).alias(x + '_df2') for x in result_df.columns))
joined_df = df1.join(df2,
                      (F.col('timestamp_df1') == F.col('timestamp_df2')) & (F.col('MMSI_df1') != F.col('MMSI_df2')),
                      "inner"
                      )

joined_df = joined_df.withColumn("distance",
                                haversine(F.col("latitude_df1"), F.col("longitude_df1"), F.col("latitude_df2"), F.col("longitude_df2")))

closest_vessels_first = joined_df.orderBy(F.col("distance")).first()

mmsi_a = closest_vessels_first["MMSI_df1"]
mmsi_b = closest_vessels_first["MMSI_df2"]
rendezvous_time = closest_vessels_first["timestamp_df1"]

after_rendezvous_obj = rendezvous_time + timedelta(minutes=10)
before_rendezvous_obj = rendezvous_time - timedelta(minutes=10)

after_rendezvous = after_rendezvous_obj.strftime("%d/%m/%Y %H:%M:%S")
before_rendezvous = before_rendezvous_obj.strftime("%d/%m/%Y %H:%M:%S")

# Get each ship trajectories
trajectory_a = result_df.where((F.col("MMSI") == mmsi_a) & F.col("timestamp").between(before_rendezvous,  after_rendezvous))
trajectory_b = result_df.where((F.col("MMSI") == mmsi_b) & F.col("timestamp").between(before_rendezvous,  after_rendezvous))

print("Two closest vessels: {} and {}".format(mmsi_a, mmsi_b))

# Convert to Pandas
trajectory_a_pd = trajectory_a.toPandas()
trajectory_b_pd = trajectory_b.toPandas()

# Visualize the trajectories of each vessel
map_center = [center_lat, center_lon]
trajectory_map = folium.Map(location=map_center, zoom_start=10)

# Add vessel A trajectory
for idx, row in trajectory_a_pd.iterrows():
    folium.CircleMarker(location=[row['latitude'], row['longitude']],
                        radius=2, color='blue').add_to(trajectory_map)

# Add vessel B trajectory
for idx, row in trajectory_b_pd.iterrows():
    folium.CircleMarker(location=[row['latitude'], row['longitude']],
                        radius=2, color='red').add_to(trajectory_map)

# Save the map to an HTML file
trajectory_map.save("vessel_trajectories.html")

# Display the map
trajectory_map

spark.stop()
