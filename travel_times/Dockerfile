FROM python:3.12-alpine
RUN pip install --no-cache-dir flask==3.0.3 requests==2.32.3 paho-mqtt==1.6.1
COPY app /app
CMD ["python", "-u", "/app/main.py"]
