import time
import csv
from azure.iot.device import IoTHubDeviceClient, Message

CONNECTION_STRING = "HostName=microgridcontrol.azure-devices.net;DeviceId=battery;SharedAccessKey=QjJx8wYDBkCtINkkalchZp11lctXL6kngAIoTK7wSwM="
DEVICE_NAME = "battery"


def iothub_client_init():
    # Create an IoT Hub client
    client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
    return client

def iothub_client_telemetry_battery_run():

    try:
        client = iothub_client_init()
        print ( "IoT Hub device BATERRY is sending messages, press Ctrl-C to exit" )
        
        with open('data.csv', 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            for row in reader:
                # Extract the two columns of data from the row
                column1, column2 = row[1], row[2]

                # Create a message with the two columns of data
                message = Message(f'{{"Battery_Active_Power": "{column1}", "Battery_Active_Power_Set_Response": "{column2}"}}')
                print( "Sending message: {}".format(message) )
                # Send the message to IoT Hub
                client.send_message(message)
                print ( "Message successfully sent" )
                # Wait for 1 second before sending the next message
                time.sleep(1)
    except KeyboardInterrupt:
        print ( "IoTHubClient sample stopped" )

if __name__ == '__main__':
    print ( "Battery - Simulated device" )
    print ( "Press Ctrl-C to exit" )
    iothub_client_telemetry_battery_run()