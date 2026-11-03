# Contenedor con Python para el simulador, el productor y el consumidor.
# Vive en la misma red que Kafka y HDFS, así resuelve los nombres kafka, namenode, dn1...
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONUNBUFFERED=1
CMD ["sleep", "infinity"]
