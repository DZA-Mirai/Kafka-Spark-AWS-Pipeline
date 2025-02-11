from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType, IntegerType

from config import configuration


def main():
    # Initialize a SparkSession for streaming application
    spark = SparkSession.builder.appName("SmartCityStreaming")\
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
                    "org.apache.hadoop:hadoop-aws:3.3.1,"
                    "com.amazonaws:aws-java-sdk:1.11.469")\
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
            .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
            .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
            .getOrCreate()

    # Adjust the log level to minimize console output
    spark.sparkContext.setLogLevel('WARN')

    # Define schemas for different types of streaming data
    # Vehicle Schema
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])

    # GPS Schema
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    # Traffic Schema
    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True)
    ])

    # Weather Schema
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True)
    ])

    # Emergency Schema
    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])

    # Function to read streaming data from a Kafka topic and apply a schema
    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')  # Kafka broker address
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes')  # Add watermark for late event handling
                )

    # Function to write streaming data to Amazon S3 in Parquet format
    def streamWriter(input_: DataFrame, checkpointFolder, output):
        return (input_.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)  # Specify checkpoint folder
                .option('path', output)     # Specify output path
                .outputMode('append')            # Append new data as it arrives
                .start())

    # Read streaming data from different Kafka topics
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

    # Data joining could be implemented here using common fields like 'id' and 'timestamp'
    # Example: joinedDF = vehicleDF.join(gpsDF, ['id', 'timestamp'])

    # Write each stream to its respective S3 path
    query1 = streamWriter(vehicleDF, checkpointFolder='s3a://spark-streaming-data-pet-project/checkpoints/vehicle_data',
                 output='s3a://spark-streaming-data-pet-project/data/vehicle_data')
    query2 = streamWriter(gpsDF, checkpointFolder='s3a://spark-streaming-data-pet-project/checkpoints/gps_data',
                 output='s3a://spark-streaming-data-pet-project/data/gps_data')
    query3 = streamWriter(trafficDF, checkpointFolder='s3a://spark-streaming-data-pet-project/checkpoints/traffic_data',
                 output='s3a://spark-streaming-data-pet-project/data/traffic_data')
    query4 = streamWriter(weatherDF, checkpointFolder='s3a://spark-streaming-data-pet-project/checkpoints/weather_data',
                 output='s3a://spark-streaming-data-pet-project/data/weather_data')
    query5 = streamWriter(emergencyDF, checkpointFolder='s3a://spark-streaming-data-pet-project/checkpoints/emergency_data',
                 output='s3a://spark-streaming-data-pet-project/data/emergency_data')

    query5.awaitTermination()


if __name__ == "__main__":
    main()