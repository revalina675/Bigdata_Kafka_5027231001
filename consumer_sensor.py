from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Buat SparkSession dengan konfigurasi Kafka
spark = SparkSession.builder \
    .appName("KafkaSensorConsumer") \
    .getOrCreate()

# Schema untuk data sensor suhu
schema_suhu = StructType([
    StructField("gudang_id", StringType()),
    StructField("suhu", IntegerType())
])

# Schema untuk data sensor kelembaban
schema_kelembaban = StructType([
    StructField("gudang_id", StringType()),
    StructField("kelembaban", IntegerType())
])

# Baca stream dari topik sensor-suhu-gudang
df_suhu = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load()

# Parsing value JSON ke kolom
df_suhu_parsed = df_suhu.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema_suhu).alias("data")) \
    .select("data.*")

# Filter suhu > 80
df_suhu_warning = df_suhu_parsed.filter(col("suhu") > 80)

# Baca stream dari topik sensor-kelembaban-gudang
df_kelembaban = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load()

# Parsing value JSON ke kolom
df_kelembaban_parsed = df_kelembaban.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema_kelembaban).alias("data")) \
    .select("data.*")

# Filter kelembaban > 70
df_kelembaban_warning = df_kelembaban_parsed.filter(col("kelembaban") > 70)

# Tampilkan hasil filter suhu > 80
query_suhu = df_suhu_warning.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Tampilkan hasil filter kelembaban > 70
query_kelembaban = df_kelembaban_warning.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Tunggu sampai stream dihentikan manual
query_suhu.awaitTermination()
query_kelembaban.awaitTermination()
