import time
import os
import uuid
import datetime
import random
import json
import csv

from azure.eventhub import EventHubProducerClient, EventData

# Create a producer client to produce and publish events to the event hub.
producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://analysisevent.servicebus.windows.net/;SharedAccessKeyName=sample;SharedAccessKey=on7aFNnRgq+myO74H2LqY7K7mHeelnODG+AEhNsLvmg=;EntityPath=battery", eventhub_name="battery")

# Open the CSV file and read each line.
with open('data.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row.
    for i, row in enumerate(reader):
        # Convert each row to a dictionary.
        reading = {'Battery_Active_Power': row[0], 'Battery_Active_Power_Set_Response': row[1]}
        s = json.dumps(reading)  # Convert the reading into a JSON string.

        # Create a batch and add event data to it.
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(s))

        # Send the batch of events to the event hub.
        producer.send_batch(event_data_batch)

        # Wait for 5 seconds before sending the next row.
        time.sleep(5)

# Close the producer.    
producer.close()
