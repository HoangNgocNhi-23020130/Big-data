import os
import time
from confluent_kafka import Producer

def main():
    print("Bắt đầu khởi động Log Producer (Chế độ Big Data với Confluent-Kafka)...")

    # Đã điền sẵn IP Tĩnh mới của bạn
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "54.152.123.50:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weblogs")
    DATASET_PATH = os.getenv("DATASET_PATH", "data/access.log") 

    # Cấu hình Producer lõi C++ (Tốc độ cực cao, kiểm tra kết nối cực ngặt)
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'python-producer',
        'compression.type': 'gzip',
        'linger.ms': 25,
        'batch.size': 65536,
        'message.timeout.ms': 10000 # Nếu sau 10s AWS không nhận, báo lỗi ngay!
    }

    try:
        producer = Producer(conf)
    except Exception as e:
        print(f"Lỗi khởi tạo Kafka Producer: {e}")
        return

    if not os.path.exists(DATASET_PATH):
        print(f"Không tìm thấy file dataset tại {DATASET_PATH}!")
        return

    print(f"Bắt đầu đọc dữ liệu từ {DATASET_PATH} và gửi lên AWS Kafka...")
    
    total_sent = 0
    start_time = time.time()

    def delivery_report(err, msg):
        if err is not None:
            print(f"Lỗi gửi tin: {err}")

    # Đọc tuần tự từng dòng để tiết kiệm RAM
    with open(DATASET_PATH, 'r', encoding='utf-8') as file:
        for line in file:
            clean_line = line.strip()
            if not clean_line:
                continue

            # Hàm produce của thư viện mới
            producer.produce(KAFKA_TOPIC, clean_line.encode('utf-8'), callback=delivery_report)
            producer.poll(0) # Bắt buộc phải có để trigger callback
            
            total_sent += 1

            if total_sent % 20000 == 0:
                # Ép gửi ngay lập tức để đo tốc độ THỰC TẾ
                producer.flush(timeout=5)
                elapsed = time.time() - start_time
                rate = total_sent / elapsed if elapsed > 0 else 0
                print(f"🚀 Đã bắn {total_sent} dòng log... Tốc độ THỰC TẾ: {rate:.2f} logs/s")

    producer.flush()
    print("Hoàn thành gửi toàn bộ dataset!")

if __name__ == "__main__":
    main()