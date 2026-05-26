import os
import json
import time
import pandas as pd
from kafka import KafkaProducer

def main():
    print("Bắt đầu khởi động Log Producer (Chế độ Big Data)...")

    # Cấu hình kết nối
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weblogs")
    DATASET_PATH = os.getenv("DATASET_PATH", "data/weblog.csv")

    # Khởi tạo Kafka Producer với cấu hình tối ưu cho throughput cao (Big Data)
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Tối ưu batching để gửi nhiều log trong 1 network request
            batch_size=32768,       # 32KB một batch
            linger_ms=20,           # Chờ tối đa 20ms để gom đủ batch trước khi gửi
            compression_type='gzip' # Nén data để giảm tải băng thông mạng
        )
    except Exception as e:
        print(f"Lỗi kết nối Kafka: {e}")
        return

    if not os.path.exists(DATASET_PATH):
        print(f"Không tìm thấy file dataset tại {DATASET_PATH}!")
        print("Vui lòng tạo thư mục log_producer/data/ và để file weblog.csv vào đó.")
        return

    print(f"Bắt đầu đọc dữ liệu từ {DATASET_PATH} và gửi lên Kafka...")
    
    # Đọc CSV theo chunk (từng phần nhỏ) để không làm treo RAM khi xử lý file vài GB
    chunk_size = 10000 
    total_sent = 0
    start_time = time.time()

    # Lưu ý: Bạn có thể cần điều chỉnh tên cột (row.IP, row.Time...) 
    # cho khớp với header thực tế của file CSV từ Kaggle.
    for chunk in pd.read_csv(DATASET_PATH, chunksize=chunk_size):
        for index, row in chunk.iterrows():
            # TODO: Map các cột từ CSV vào cấu trúc Data Contract
            # Dưới đây là ví dụ mapping giả định tên cột trong file CSV
            log_entry = {
                "ip_address": str(row.get('IP', '0.0.0.0')),
                "timestamp": str(row.get('Time', '2019-01-01T00:00:00+00:00')),
                "http_method": str(row.get('Method', 'GET')),
                "endpoint": str(row.get('URL', '/')),
                "protocol": "HTTP/1.1",
                "status_code": int(row.get('Status', 200)),
                "response_size_bytes": str(row.get('Size', '0')), # Sẽ xử lý "-" ở Spark
                "referer": str(row.get('Referer', '-')),
                "user_agent": str(row.get('UserAgent', '-'))
            }

            # Gửi không đồng bộ (asynchronous) vào Kafka
            producer.send(KAFKA_TOPIC, value=log_entry)
            total_sent += 1

        # Cứ sau mỗi chunk (10,000 dòng), ép Kafka gửi toàn bộ buffer đi
        producer.flush()
        
        elapsed = time.time() - start_time
        rate = total_sent / elapsed if elapsed > 0 else 0
        print(f"Đã gửi {total_sent} logs... Tốc độ: {rate:.2f} logs/s")
        
        # KHÔNG DÙNG time.sleep(0.5) cho mỗi dòng. 
        # Tùy chọn: Bạn có thể bật sleep 아주 ngắn ở mức Chunk (vd: 0.1s) 
        # nếu tốc độ push quá khủng khiếp làm chết Kafka local của bạn.
        # time.sleep(0.1)

    producer.close()
    print("Hoàn thành gửi toàn bộ dataset!")

if __name__ == "__main__":
    main()
