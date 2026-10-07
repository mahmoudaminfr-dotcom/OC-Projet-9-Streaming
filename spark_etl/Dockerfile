FROM python:3.9-slim
WORKDIR /app
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY spark_processor.py .
CMD ["python", "-u", "spark_processor.py"]
