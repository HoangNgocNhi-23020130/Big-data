# Đóng gói script thành image chạy trên K8s
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY generate_logs.py .
CMD ["python", "generate_logs.py"]
