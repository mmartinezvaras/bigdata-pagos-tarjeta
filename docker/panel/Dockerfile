# Panel visual del sistema (Streamlit). Lee Kafka, HDFS y Docker y lanza las mismas piezas
# que la terminal (productor, .sql de Hive, oráculo y comparador).
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config.toml /root/.streamlit/config.toml
ENV PYTHONUNBUFFERED=1
EXPOSE 8501
CMD ["streamlit", "run", "app.py"]
