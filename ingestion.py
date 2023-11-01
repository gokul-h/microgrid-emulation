import time
import os
import uuid
import datetime
import random
import json
import csv

from azure.eventhub import EventHubProducerClient, EventData

# Create a producer client to produce and publish events to the event hub.
producer = EventHubProducerClient.from_connection_string(
    conn_str="Endpoint=sb://microgrid.servicebus.windows.net/;SharedAccessKeyName=grid;SharedAccessKey=4w9ZjvDa8OPNhANa3Mpkx4nzCmt90XbQi+AEhJCcxt8=;EntityPath=grid", eventhub_name="grid")

# Open the CSV file and read each line.
with open('NOV_2022.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row.
    for i, row in enumerate(reader):
        # Convert each row to a dictionary.
        reading = {'Timestamp': row[0],
                   'Battery_Active_Power': row[1],
                   'Battery_Active_Power_Set_Response': row[2],
                   'PVPCS_Active_Power': row[3],
                   'GE_Body_Active_Power': row[4],
                   'GE_Active_Power': row[5],
                   'GE_Body_Active_Power_Set_Response': row[6],
                   'FC_Active_Power_FC_END_Set': row[7],
                   'FC_Active_Power': row[8],
                   'FC_Active_Power_FC_end_Set_Response': row[9],
                   'Island_mode_MCCB_Active_Power': row[10],
                   'MG-LV-MSB_AC_Voltage': row[11],
                   'Receiving_Point_AC_Voltage': row[12],
                   'Island_mode_MCCB_AC_Voltage': row[13],
                   'Island_mode_MCCB_Frequency': row[14],
                   'MG-LV-MSB_Frequency': row[15],
                   'Inlet_Temperature_of_Chilled_Water': row[16],
                   'Outlet_Temperature': row[17]
                   }
        s = json.dumps(reading)  # Convert the reading into a JSON string.
        # debugging line
        print("Sending data ", i, " ", row[0])
        # Create a batch and add event data to it.
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(s))

        # Send the batch of events to the event hub.
        producer.send_batch(event_data_batch)

        # Wait for 5 seconds before sending the next row.
        time.sleep(5)

# Close the producer.
producer.close()
