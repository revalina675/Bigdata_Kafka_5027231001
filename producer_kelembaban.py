from kafka import KafkaProducer
import json
import time
import random

# Membuat Kafka Producer yang terhubung ke Kafka broker di localhost:9092
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Mengubah data ke format JSON bytes
)

# Daftar gudang yang akan dikirimi data sensor kelembaban
gudang_list = ["G1", "G2", "G3"]

print("Mulai mengirim data kelembaban ke Kafka...")

while True:
    # Membuat data kelembaban secara acak untuk salah satu gudang
    data = {
        "gudang_id": random.choice(gudang_list),
        "kelembaban": random.randint(60, 80)  # kelembaban acak antara 60% sampai 80%
    }

    try:
        # Kirim data ke topik sensor-kelembaban-gudang
        producer.send('sensor-kelembaban-gudang', value=data)
        producer.flush()  # Memastikan data langsung dikirim
        print(f"Data terkirim: {data}")
    except Exception as e:
        print(f"Error mengirim data: {e}")

    time.sleep(1)  # jeda 1 detik sebelum mengirim data berikutnya
