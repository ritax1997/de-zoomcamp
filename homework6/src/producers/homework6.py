import json
import csv
import pandas as pd
from kafka import KafkaProducer
from time import time

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

# Check if connected successfully
connection_result = producer.bootstrap_connected()
print(f"Connection to Kafka server: {connection_result}")
if connection_result:
    # Load the green trip data
    csv_file = 'green_tripdata_2019-10.csv'
    topic_name = 'green-trips'
    
    # Define the columns we want to keep
    columns_to_keep = [
        'lpep_pickup_datetime',
        'lpep_dropoff_datetime',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'tip_amount'
    ]
    
    # Start timing
    t0 = time()
    
    # Read and process the CSV file
    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        # Counter for tracking progress
        counter = 0
        
        for row in reader:
            # Filter to keep only the columns we want
            filtered_row = {col: row[col] for col in columns_to_keep if col in row}
            
            # Convert string values to appropriate types
            try:
                # For PULocationID and DOLocationID
                filtered_row['PULocationID'] = int(filtered_row['PULocationID']) if filtered_row['PULocationID'].strip() else 0
                filtered_row['DOLocationID'] = int(filtered_row['DOLocationID']) if filtered_row['DOLocationID'].strip() else 0
                
                # For numeric values that could be empty
                filtered_row['passenger_count'] = float(filtered_row['passenger_count']) if filtered_row['passenger_count'].strip() else 0.0
                filtered_row['trip_distance'] = float(filtered_row['trip_distance']) if filtered_row['trip_distance'].strip() else 0.0
                filtered_row['tip_amount'] = float(filtered_row['tip_amount']) if filtered_row['tip_amount'].strip() else 0.0
                
                # Keep datetime fields as strings for JSON serialization
            except (ValueError, KeyError) as e:
                print(f"Error processing row {counter}: {e}")
                continue
            
            # Send the message to Kafka
            producer.send(topic_name, value=filtered_row)
            
            # Print progress periodically
            counter += 1
            if counter % 10000 == 0:
                print(f"Sent {counter} messages so far")
    
    # Flush to ensure all messages are sent
    producer.flush()
    
    # End timing
    t1 = time()
    
    print(f"Total time taken: {(t1- t0):.2f} seconds")
    print(f"Sent {counter} messages in total")
else:
    print("Failed to connect to the Kafka server. Please check your configuration.")