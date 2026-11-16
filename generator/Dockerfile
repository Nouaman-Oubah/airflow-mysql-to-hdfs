FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir faker==30.8.2 mysql-connector-python==9.1.0
COPY generator.py .

ENV PYTHONUNBUFFERED=1
CMD ["python", "generator.py"]
