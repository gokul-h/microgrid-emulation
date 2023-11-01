FROM python:3.8-slim-buster

RUN pip install azure-storage-blob azure-eventhub

COPY battery_new_event.py .

CMD ["python", "battery_new_event.py"]