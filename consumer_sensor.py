from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Inisialisasi SparkSession dengan Kafka package
spark = SparkSession.builder \
    .appName("KafkaSensorConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Skema data suhu dan kelembaban
schema_suhu = StructType([
    StructField("gudang_id", StringType()),
    StructField("suhu", IntegerType())
])

schema_kelembaban = StructType([
    StructField("gudang_id", StringType()),
    StructField("kelembaban", IntegerType())
])

# Baca stream suhu dari Kafka
suhu_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load()

suhu_parsed = suhu_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema_suhu).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())

# Baca stream kelembaban dari Kafka
kelembaban_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load()

kelembaban_parsed = kelembaban_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema_kelembaban).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())

# Tambahkan watermark
suhu_with_watermark = suhu_parsed.withWatermark("timestamp", "20 seconds")
kelembaban_with_watermark = kelembaban_parsed.withWatermark("timestamp", "20 seconds")

# Join berdasarkan gudang_id dan rentang waktu 10 detik
joined_df = suhu_with_watermark.join(
    kelembaban_with_watermark,
    on=(
        (suhu_with_watermark.gudang_id == kelembaban_with_watermark.gudang_id) &
        (suhu_with_watermark.timestamp >= kelembaban_with_watermark.timestamp - expr("interval 10 seconds")) &
        (suhu_with_watermark.timestamp <= kelembaban_with_watermark.timestamp + expr("interval 10 seconds"))
    ),
    how="inner"
)

# Tambah kolom status berdasarkan kondisi suhu dan kelembaban
result_df = joined_df.select(
    suhu_with_watermark.gudang_id,
    suhu_with_watermark.suhu,
    kelembaban_with_watermark.kelembaban,
    suhu_with_watermark.timestamp.alias("timestamp_suhu"),
    kelembaban_with_watermark.timestamp.alias("timestamp_kelembaban")
).withColumn(
    "status",
    when((col("suhu") > 80) & (col("kelembaban") > 70),
         "[PERINGATAN KRITIS] Bahaya tinggi! Barang berisiko rusak")
    .when(col("suhu") > 80,
          "[Peringatan Suhu Tinggi] Suhu tinggi, kelembaban normal")
    .when(col("kelembaban") > 70,
          "[Peringatan Kelembaban Tinggi] Kelembaban tinggi, suhu aman")
    .otherwise("[Aman] Tidak ada peringatan")
)

# Fungsi custom untuk mencetak hasil dalam format teks baris
def format_and_print(batch_df, epoch_id):
    records = batch_df.collect()
    if not records:
        return

    print("\n[HASIL MONITORING GUDANG]")
    for row in records:
        print(f"\nGudang {row['gudang_id']}:")
        print(f"- Suhu: {row['suhu']}°C")
        print(f"- Kelembaban: {row['kelembaban']}%")
        print(f"- Status: {row['status']}")

# Streaming dengan sink foreachBatch untuk output custom
query = result_df.writeStream \
    .foreachBatch(format_and_print) \
    .outputMode("append") \
    .start()

query.awaitTermination()
