import os
import json
import time
import re
from datetime import datetime
from kafka import KafkaProducer

# Regex chuẩn cho Apache/Nginx Combined Access Log
# Mẫu: 54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "GET /filter/27 HTTP/1.1" 200 30577 "-" "Mozilla/5.0..."
LOG_REGEX = re.compile(
    r'^(?P<ip>\S+) \S+ \S+ \[(?P<time>[^\]]+)\] "(?P<method>[A-Z]+) (?P<endpoint>[^ "]+) (?P<protocol>HTTP/[0-9.]+)" (?P<status>\d{3}) (?P<size>\d+|-) "(?P<referer>[^"]*)" "(?P<agent>[^"]*)"'
)

def parse_log_line(line):
    match = LOG_REGEX.match(line)
    if not match:
        return None
    
    data = match.groupdict()
    
    # Convert thời gian từ chuẩn Apache (22/Jan/2019:03:56:14 +0330) sang chuẩn ISO8601 cho Elasticsearch
    try:
        # Tách phần múi giờ ra để xử lý nếu cần, hoặc dùng strptime
        dt_obj = datetime.strptime(data['time'], "%d/%b/%Y:%H:%M:%S %z")
        iso_time = dt_obj.isoformat()
    except Exception:
        iso_time = data['time'] # Lỗi thì giữ nguyên cho Spark xử lý

    return {
        "ip_address": data['ip'],
        "timestamp": iso_time,
        "http_method": data['method'],
        "endpoint": data['endpoint'],
        "protocol": data['protocol'],
        "status_code": int(data['status']),
        "response_size_bytes": data['size'], # Trả về '-' hoặc số, Spark sẽ tự ép kiểu
        "referer": data['referer'],
        "user_agent": data['agent']
    }

def main():
    print("Bắt đầu khởi động Log Producer (Đọc file .log thô)...")

    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weblogs")
    # Đổi đuôi mặc định sang file thực tế của bạn
    DATASET_PATH = os.getenv("DATASET_PATH", "data/access.log")

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            batch_size=32768,       
            linger_ms=20,           
            compression_type='gzip' 
        )
    except Exception as e:
        print(f"Lỗi kết nối Kafka: {e}")
        return

    if not os.path.exists(DATASET_PATH):
        print(f"Không tìm thấy file tại {DATASET_PATH}!")
        return

    print(f"Bắt đầu đọc dữ liệu từ {DATASET_PATH} và gửi lên Kafka...")
    
    total_sent = 0
    start_time = time.time()

    # Mở file đọc từng dòng (tiết kiệm RAM tuyệt đối, 100GB cũng đọc được)
    with open(DATASET_PATH, 'r', encoding='utf-8', errors='ignore') as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue

            log_entry = parse_log_line(line)
            
            if log_entry:
                producer.send(KAFKA_TOPIC, value=log_entry)
                total_sent += 1
                
                # Gửi buffer đi mỗi 10.000 dòng
                if total_sent % 10000 == 0:
                    producer.flush()
                    elapsed = time.time() - start_time
                    rate = total_sent / elapsed if elapsed > 0 else 0
                    print(f"Đã gửi {total_sent} logs... Tốc độ: {rate:.2f} logs/s")

    producer.flush()
    producer.close()
    print(f"Hoàn thành gửi toàn bộ dataset! Tổng cộng: {total_sent} logs.")

if __name__ == "__main__":
    main()
