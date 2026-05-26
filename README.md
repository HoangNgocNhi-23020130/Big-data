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
(Đang cập nhật...)