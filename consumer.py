import json
from kafka import KafkaConsumer

# 1. Kafka Consumer එක සෙට් කිරීම
consumer = KafkaConsumer(
    'crypto-prices', # අපි කියවන්න ඕනේ Topic එක
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', # මුල ඉඳන්ම දත්ත කියවන්න පටන් ගන්න
    enable_auto_commit=True,
    group_id='my-group', # Consumer Group එකක් දාන්න ඕනේ
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) # දත්ත Decode කරලා JSON කරන්න
)

print("Consumer started... Listening to 'crypto-prices' topic.")

# 2. දිගටම දත්ත කියවීමේ Loop එක
try:
    for message in consumer:
        # Kafka එකෙන් ලැබෙන පණිවිඩයේ ඇතුළත තියෙන දත්ත (Value) ගනිමු
        data = message.value
        
        # දත්ත ටික පිරිසිදු කරලා අපිට ඕන විදිහට ගනිමු
        symbol = data['symbol']
        price = data['price']
        timestamp = data['timestamp']
        
        print(f"Received from Kafka: {symbol} is ${price} at {timestamp}")

except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    consumer.close()