import serial
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from cassandra.cluster import Cluster
import json
import logging
import pandas as pd
import matplotlib.pyplot as plt
import tkinter as tk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

power_bi_push_url = 'YOUR_POWER_BI_PUSH_URL'
batch_size = 100
serial_port = '/dev/tty.usbmodem1234561'
baudrate = 115200

ser = serial.Serial(serial_port, baudrate, timeout=1)
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
cluster = Cluster(['localhost'])
session = cluster.connect('sensor_data_analytics')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize data structures for storing received data
data_points = []

def process_data(data):
    try:
        data = json.loads(data)
        data_points.append(data)
        update_dashboard()
        send_to_kafka(data)
        send_to_power_bi(data)
        insert_to_cassandra(data)
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON: {e}")
        return None

def send_to_kafka(data):
    future = producer.send('sensor_data', value=json.dumps(data).encode())
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError as e:
        logger.error(f"Error sending data to Kafka: {e}")

def send_to_power_bi(data):
    payload = [{'SensorData': data}]
    response = requests.post(power_bi_push_url, json=payload)
    if response.status_code == 200:
        logger.info("Data sent to Power BI successfully.")
    else:
        logger.error(f"Failed to send data to Power BI: {response.text}")

def insert_to_cassandra(data):
    session.execute("""
        INSERT INTO sensor_data (timestamp, reed_state, temperature, humidity)
        VALUES (toTimestamp(now()), ?, ?, ?)
    """, (data['reed_state'], data['temperature'], data['humidity']))

def update_dashboard():
    # Create a pandas DataFrame from the received data points
    df = pd.DataFrame(data_points)

    # Create a new figure
    fig = plt.Figure(figsize=(10, 5))

    # Create a subplot for the line chart
    line_chart = fig.add_subplot(121)

    # Create a line chart of temperature data over time
    line_chart.plot(df['timestamp'], df['temperature'])
    line_chart.set_xlabel('Time')
    line_chart.set_ylabel('Temperature')
    line_chart.set_title('Temperature Over Time')

    # Create a subplot for the scatter plot
    scatter_chart = fig.add_subplot(122)

    # Create a scatter plot of humidity vs. temperature
    scatter_chart.scatter(df['humidity'], df['temperature'])
    scatter_chart.set_xlabel('Humidity')
    scatter_chart.set_ylabel('Temperature')
    scatter_chart.set_title('Humidity vs. Temperature')

    # Create a tkinter canvas for the figure
    canvas = FigureCanvasTkAgg(fig, master=root)
    canvas.get_tk_widget().grid(row=0, column=0, columnspan=2)

# Create a tkinter GUI
root = tk.Tk()

# Start the main loop
root.mainloop()

# Close the Kafka producer
producer.close()

# Close the Cassandra session
session.shutdown()
