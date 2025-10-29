from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, S>

spark = SparkSession.builder \
    .appName("KafkaSparkStreamingFacturas") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("ID_Factura", IntegerType()),
    StructField("Fecha", StringType()),
    StructField("SKU", StringType()),
    StructField("Cantidad", IntegerType()),
    StructField("Precio", FloatType())
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "facturas_data") \
    .load()

parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data>
parsed_df = parsed_df.withColumn("Fecha", col("Fecha").cast(TimestampType()))
parsed_df = parsed_df.withColumn("Valor_Total", expr("Cantidad * Precio"))

query = parsed_df.select("ID_Factura", "Fecha", "SKU", "Cantidad", "Precio", "Va>
    .writeStream \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
