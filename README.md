# Hệ thống phân tích dữ liệu nhật ký Web (Web Server Access Logs)

Hệ thống sử dụng Kafka, Spark Streaming, Elasticsearch và Kibana để xử lý dữ liệu log web theo thời gian thực.

## Cấu trúc dự án
- `docs/`: Tài liệu kiến trúc và data contract
- `k8s_manifests/`: Chứa các file YAML để deploy lên Kubernetes
- `log_producer/`: Script sinh log và đẩy vào Kafka
- `spark_streaming/`: Ứng dụng PySpark đọc từ Kafka và ghi vào Elasticsearch
- `elasticsearch/`: Cấu hình index mapping
- `kibana/`: Backup file dashboard
- `qa_tests/`: Script kiểm thử dữ liệu và lỗi hạ tầng

## Hướng dẫn chung cách setup toàn bộ hệ thống
- SSH: ssh -i "key.pem" ubuntu@54.152.123.50

- Tạo Index weblogs (Lưu log web đã xử lý sạch):
`
kubectl exec deployment/elasticsearch -- curl -s -X PUT -H 'Content-Type: application/json' http://localhost:9200/weblogs -d '{
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
}'
`
- Tạo Index security_alerts (Lưu cảnh báo DDoS đột biến):
`
kubectl exec deployment/elasticsearch -- curl -s -X PUT -H 'Content-Type: application/json' http://localhost:9200/security_alerts -d '{
 "mappings": {
   "properties": {
     "alert_type": { "type": "keyword" },
     "attack_time": { "type": "date" },
     "ip_address": { "type": "ip" },
     "request_count": { "type": "integer" }
   }
 }
}'
`
