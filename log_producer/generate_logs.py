import os
import time
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
    print("Bắt đầu khởi động Log Producer (Chế độ Big Data - Raw Stream)...")

    # Cấu hình kết nối lấy từ Environment hoặc mặc định
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "54.152.123.50:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weblogs")
    # Đổi tên file mặc định từ .csv sang .log cho đúng thực tế Kaggle
    DATASET_PATH = os.getenv("DATASET_PATH", "access.log/access.log") 

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            # Vì gửi chuỗi thô (dòng log Nginx), ta mã hóa trực tiếp sang định dạng bytes
            value_serializer=lambda v: v.encode('utf-8'),
            batch_size=65536,        # Tăng lên 64KB một batch để tối ưu đường truyền internet lên AWS
            linger_ms=25,           # Chờ tối đa 25ms để gom cụm dữ liệu
            compression_type='gzip' # Nén tối đa băng thông đường truyền mạng
        )
    except Exception as e:
        print(f"Lỗi kết nối Kafka: {e}")
        return

    if not os.path.exists(DATASET_PATH):
        print(f"Không tìm thấy file log tại {DATASET_PATH}!")
        print("Vui lòng để file access.log vào thư mục log_producer/data/")
        return

    print(f"Đang đọc dữ liệu từ {DATASET_PATH} và đẩy luồng lên AWS Kafka...")
    
    total_sent = 0
    start_time = time.time()

    # Đọc luồng tuần tự từng dòng (Streaming) - Giải pháp tối ưu nhất cho file nhiều GB
    with open(DATASET_PATH, 'r', encoding='utf-8') as file:
        for line in file:
            clean_line = line.strip()
            if not clean_line:
                continue

            # Gửi không đồng bộ dòng log thô lên Kafka
            producer.send(KAFKA_TOPIC, value=clean_line)
            total_sent += 1

            # Cứ mỗi 20,000 dòng thì in báo cáo tốc độ và ép giải phóng bộ nhớ đệm
            if total_sent % 20000 == 0:
                producer.flush()
                elapsed = time.time() - start_time
                rate = total_sent / elapsed if elapsed > 0 else 0
                print(f"🚀 Đã bắn {total_sent} dòng log... Tốc độ hiện tại: {rate:.2f} logs/s")

    # Giải phóng hoàn toàn các gói tin còn sót lại trước khi tắt ứng dụng
    producer.flush()
    producer.close()
    print(f"✨ Hoàn thành! Toàn bộ {total_sent} dòng log đã được nạp vào Kafka.")

if __name__ == "__main__":
    main()