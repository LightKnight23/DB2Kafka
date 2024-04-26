import serial
import json
from kafka import KafkaProducer
from cassandra.cluster import Cluster
import logging

batch_size = 100
serial_port = '/dev/tty.usbmodem1234561'
baudrate = 115200

ser = serial.Serial(serial_port, baudrate, timeout=1)
cluster = Cluster(['localhost'])
session = cluster.connect('sensor_data_analytics')

# Function to create table if it doesn't exist
def create_table_if_not_exists(session, keyspace_name, table_name):
    keyspace_metadata = session.cluster.metadata.keyspaces.get(keyspace_name)
    if keyspace_metadata is not None and table_name not in keyspace_metadata.tables:
        session.execute(f"""
            CREATE TABLE {keyspace_name}.{table_name} (
                timestamp timestamp,
                reed_state text,
                temperature double,
                humidity double,
                PRIMARY KEY (timestamp)
            )
        """)

# Check and create table if it doesn't exist
create_table_if_not_exists(session, 'sensor_data_analytics', 'sensor_data')

prepared_stmt = session.prepare("""
    INSERT INTO sensor_data_analytics.sensor_data (timestamp, reed_state, temperature, humidity)
    VALUES (toTimestamp(now()), ?, ?, ?)
""")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_data(data):
    try:
        # Add the received data to an empty dictionary
        data_dict = {}
        data_dict['reed_state'] = data['reed_state']
        data_dict['temperature'] = data['temperature']
        data_dict['humidity'] = data['humidity']

        # Convert the dictionary to JSON
        json_data = json.dumps(data_dict)

        # Send the JSON data to Kafka
        producer.send('sensor_data', json_data.encode())

        # Insert the data into Cassandra
        insert_to_cassandra(data_dict)
        print("Data inserted into Cassandra:", data_dict)
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise Exception("Error processing data") from e

def insert_to_cassandra(data):
    try:
        session.execute(prepared_stmt, (data['reed_state'], data['temperature'], data['humidity']))
    except Exception as e:
        logger.error(f"Error inserting data into Cassandra: {e}")
        raise Exception("Failed to insert data into Cassandra") from e

# Start the UART communication
ser.flushInput()
ser.flushOutput()

buffer = ""

# Start the Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

while True:
    try:
        # Read data from the UART port
        data = ser.readline().decode()

        if data.strip():
            print("Received data:", data.strip())
            # Add the received data to the buffer
            buffer += data

        # Check if the buffer ends with a newline character
        if buffer.endswith("\n"):
            # Process the received data
            received_data = json.loads(buffer.strip())
            process_data(received_data)
            buffer = ""
    except (ValueError, Exception) as e:
        logger.error(f"Error processing data: {e}")
        continue

# # Close the Cassandra session and cluster
# session.shutdown()
# cluster.shutdown()