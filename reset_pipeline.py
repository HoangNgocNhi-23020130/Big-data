import subprocess
import json
import base64
import time

# Config mappings
weblogs_json = """{
  "mappings": {
    "properties": {
      "ip_address": { "type": "ip" },
      "timestamp": { "type": "date" },
      "http_method": { "type": "keyword" },
      "endpoint": { "type": "keyword" },
      "protocol": { "type": "keyword" },
      "status_code": { "type": "integer" },
      "response_size_bytes": { "type": "integer" },
      "referer": { "type": "keyword" },
      "user_agent": { "type": "text" }
    }
  }
}"""

security_alerts_json = """{
  "mappings": {
    "properties": {
      "alert_type": { "type": "keyword" },
      "attack_time": { "type": "date" },
      "ip_address": { "type": "ip" },
      "request_count": { "type": "integer" }
    }
  }
}"""

weblogs_compact = json.dumps(json.loads(weblogs_json))
security_alerts_compact = json.dumps(json.loads(security_alerts_json))

def run_remote(command, check=True):
    ssh_cmd = ["ssh", "-i", "key.pem", "-o", "StrictHostKeyChecking=no", "ubuntu@54.152.123.50", command]
    res = subprocess.run(ssh_cmd, capture_output=True, text=True)
    if check and res.returncode != 0:
        print(f"⚠️ Cảnh báo/Lỗi lệnh: {command}\nSTDERR: {res.stderr.strip()}")
    return res.stdout, res.stderr

def main():
    print("🔄 Bắt đầu reset toàn bộ hệ thống (Kafka, Spark, Elasticsearch)...")

    # 1. Scale down Spark Streaming
    print("⏳ 1. Dừng Spark Streaming...")
    run_remote("kubectl scale deployment spark-streaming-processor --replicas=0")
    
    # Chờ cho đến khi các Pod Spark Streaming tắt hoàn toàn
    print("⏳ Đang chờ các Pod Spark Streaming tắt hẳn...")
    for _ in range(20):
        stdout, _ = run_remote("kubectl get pods -l app=spark-streaming 2>/dev/null", check=False)
        if "spark-streaming" not in stdout:
            break
        time.sleep(1)
    else:
        print("⚠️ Cảnh báo: Các Pod Spark Streaming chưa tắt hoàn toàn, vẫn tiến hành bước tiếp theo...")

    # 2. Xóa và tạo lại Kafka Topic
    print("⏳ 2. Xóa hàng đợi Kafka cũ...")
    run_remote("kubectl exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic weblogs 2>/dev/null", check=False)
    
    # Chờ cho đến khi topic thực sự bị xóa hoàn toàn khỏi Kafka
    print("⏳ Đang chờ Kafka hoàn tất việc xóa topic cũ...")
    for i in range(15):
        stdout, _ = run_remote("kubectl exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list", check=False)
        if "weblogs" not in stdout.split():
            break
        time.sleep(1)
    else:
        print("⚠️ Cảnh báo: Kafka chưa kịp hoàn tất xóa topic cũ, tiếp tục thử tạo...")

    print("⏳ Tạo hàng đợi Kafka sạch mới...")
    run_remote("kubectl exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic weblogs --partitions 3 --replication-factor 1")

    # 3. Xóa và tạo lại Elasticsearch Indexes với mapping chuẩn
    print("⏳ 3. Xóa index Elasticsearch cũ...")
    run_remote("kubectl exec deployment/elasticsearch -- curl -s -X DELETE http://localhost:9200/weblogs", check=False)
    run_remote("kubectl exec deployment/elasticsearch -- curl -s -X DELETE http://localhost:9200/security_alerts", check=False)
    time.sleep(2)

    print("⏳ Tạo index weblogs mới với mapping chuẩn...")
    run_remote(f"kubectl exec deployment/elasticsearch -- curl -s -X PUT -H 'Content-Type: application/json' http://localhost:9200/weblogs -d '{weblogs_compact}'")

    print("⏳ Tạo index security_alerts mới với mapping chuẩn...")
    run_remote(f"kubectl exec deployment/elasticsearch -- curl -s -X PUT -H 'Content-Type: application/json' http://localhost:9200/security_alerts -d '{security_alerts_compact}'")

    # 4. Scale up Spark Streaming
    print("⏳ 4. Khởi động lại Spark Streaming...")
    run_remote("kubectl scale deployment spark-streaming-processor --replicas=1")

    print("\n✅ Reset hệ thống THÀNH CÔNG RỰC RỠ! Toàn bộ dữ liệu cũ đã được xóa sạch.")
    print("🚀 Bây giờ bạn có thể bắt đầu chạy `python generate_logs.py` để bắn dữ liệu mới từ con số 0.")

if __name__ == "__main__":
    main()
