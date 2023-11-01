import csv
import json
import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string("Endpoint=sb://analysisevent.servicebus.windows.net/;SharedAccessKeyName=sample;SharedAccessKey=on7aFNnRgq+myO74H2LqY7K7mHeelnODG+AEhNsLvmg=;EntityPath=battery", eventhub_name="analysisevent")
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Read the CSV file and create a list of dictionaries.
        data = []
        with open('data.csv', mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                data.append(row)

        # Iterate through the list of dictionaries and create a JSON string for each row.
        for row in data:
            json_string = json.dumps(row)

            # Add the JSON string to the batch.
            event_data_batch.add(EventData(json_string))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
