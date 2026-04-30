import time
import json
import requests
from kafka import KafkaProducer

# 1. Kafka Setup (Define where the messages are sent)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    # Serialize our data into JSON format before sending to Kafka
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def get_crypto_price():
    """Function to fetch live Bitcoin price from the Binance API"""
    url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    try:
        response = requests.get(url)
        data = response.json()
        # Add the current local timestamp to the data for tracking
        data['timestamp'] = time.time()
        return data
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

# 2. Continuous Data Ingestion Loop
print("Starting Kafka Producer... (Press Ctrl+C to stop)")

try:
    while True:
        price_data = get_crypto_price()
        
        if price_data:
            # Send the data to the Kafka topic named 'crypto-prices'
            producer.send('crypto-prices', value=price_data)
            print(f"Sent to Kafka: {price_data}")
            
        # Wait for 3 seconds before fetching the next update
        time.sleep(3)
except KeyboardInterrupt:
    print("Producer stopped.")
finally:
    # Safely close the producer connection
    producer.close()