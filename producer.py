import time
import json
import requests
from kafka import KafkaProducer

# 1. Kafka සෙට් කිරීම (පණිවිඩ යවන්නේ කොහෙටද?)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    # අපේ දත්ත JSON විදිහට Encode කරලා යවන්න මේක ඕනේ
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def get_crypto_price():
    """Binance API එකෙන් Bitcoin වල ලයිව් මිල ගන්නා Function එක"""
    url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    try:
        response = requests.get(url)
        data = response.json()
        # අපිට ලැබෙන දත්ත වලට වර්තමාන වෙලාවත් (Timestamp) එකතු කරමු
        data['timestamp'] = time.time()
        return data
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

# 2. දිගටම දත්ත යැවීමේ Loop එක
print("Starting Kafka Producer... (Press Ctrl+C to stop)")

try:
    while True:
        price_data = get_crypto_price()
        
        if price_data:
            # 'crypto-prices' කියන topic එකට data යවනවා
            producer.send('crypto-prices', value=price_data)
            print(f"Sent to Kafka: {price_data}")
            
        # සෑම තත්පර 3කට වරක්ම මේක කරන්න
        time.sleep(3)
except KeyboardInterrupt:
    print("Producer stopped.")
finally:
    producer.close()