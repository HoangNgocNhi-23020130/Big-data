from confluent_kafka import Consumer

print("Đang kết nối lên AWS để kéo log về...")

# Cấu hình người tiêu thụ (Consumer)
conf = {
    'bootstrap.servers': '54.152.123.50:9092',
    'group.id': 'team4-test-group',
    'auto.offset.reset': 'earliest' # Lệnh này bảo Kafka: "Hãy nhả data từ dòng đầu tiên ra đây"
}

consumer = Consumer(conf)
consumer.subscribe(['weblogs'])

# Chỉ lấy 10 dòng ra xem thử rồi đóng
for i in range(10):
    msg = consumer.poll(5.0) # Chờ tối đa 5 giây
    
    if msg is None:
        print("Đang đợi dữ liệu...")
        continue
    if msg.error():
        print(f"Lỗi: {msg.error()}")
        continue
        
    # In ra dòng log thô đã nhận được
    print(f"✅ Đã nhận: {msg.value().decode('utf-8')[:120]}...")

consumer.close()
print("Test thành công rực rỡ!")