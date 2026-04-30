import json
from kafka import KafkaConsumer

# 1. Kafka Consumer configuration
consumer = KafkaConsumer(
    'crypto-prices', # The topic we want to subscribe to
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', # Start reading from the earliest available message
    enable_auto_commit=True,
    group_id='my-group', # Assign a unique Consumer Group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserialize data from bytes to JSON format
)

print("Consumer started... Listening to 'crypto-prices' topic.")

# 2. Continuous loop to read data from Kafka
try:
    for message in consumer:
        # Extract the actual data (Value) from the Kafka message
        data = message.value
        
        # Extract and assign specific fields from the data
        symbol = data['symbol']
        price = data['price']
        timestamp = data['timestamp']
        
        print(f"Received from Kafka: {symbol} is ${price} at {timestamp}")

except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    # Safely close the consumer connection
    consumer.close()