import json
import time
import random
from kafka import KafkaProducer, KafkaConsumer
import requests

# === CONFIG ===
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'ctrm-topic'
PUSH_URL = 'YOUR_PUSH_URL_HERE'  # From Power BI
HEADERS = {'Content-Type': 'application/json'}

# === KAFKA SETUP ===
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10,  # Batch for efficiency
    batch_size=16384
)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='ctrm-producer-group',
    consumer_timeout_ms=1000  # Prevent infinite block
)

# === STATE ===
commodities = ['Oil', 'Gold', 'Wheat']
current_prices = {comm: 100.0 for comm in commodities}
trade_counter = 0
open_trades = {}

def calculate_pnl(trade, current_price, is_close=False):
    if trade['position_type'] == 'Long':
        diff = (current_price - trade['entry_price']) if not is_close else (trade['close_price'] - trade['entry_price'])
    else:
        diff = (trade['entry_price'] - current_price) if not is_close else (trade['entry_price'] - trade['close_price'])
    return round(diff * trade['quantity'], 2)

# === INITIAL PRICES ===
for comm in commodities:
    price_data = {
        'type': 'price',
        'commodity': comm,
        'price': current_prices[comm],
        'volume': round(random.uniform(1000, 5000), 2)
    }
    producer.send(TOPIC, price_data)

print("CTRM Producer started. Streaming to Power BI...")

# === MAIN LOOP ===
try:
    while True:
        # === PRODUCE DATA ===
        for comm in commodities:
            current_prices[comm] += random.uniform(-5, 5)
            price_data = {
                'type': 'price',
                'commodity': comm,
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'price': round(current_prices[comm], 2),
                'volume': round(random.uniform(1000, 5000), 2)
            }
            producer.send(TOPIC, price_data)

        # New trade?
        if random.random() > 0.7:
            trade_counter += 1
            comm = random.choice(commodities)
            trade = {
                'type': 'trade',
                'trade_id': trade_counter,
                'commodity': comm,
                'quantity': round(random.uniform(100, 1000), 2),
                'entry_price': round(current_prices[comm] + random.uniform(-10, 10), 2),
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'status': 'Open',
                'position_type': random.choice(['Long', 'Short'])
            }
            producer.send(TOPIC, trade)
            open_trades[trade_counter] = trade

        # Close trade?
        if open_trades and random.random() > 0.8:
            close_id = random.choice(list(open_trades.keys()))
            trade = open_trades.pop(close_id)
            trade['status'] = 'Closed'
            trade['close_price'] = round(current_prices[trade['commodity']], 2)
            trade['close_timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%SZ')
            producer.send(TOPIC, trade)

        # === CONSUME & PUSH TO POWER BI ===
        # Poll for up to 1 second
        for message in consumer:
            data = message.value
            if data['type'] == 'price':
                current_prices[data['commodity']] = data['price']

            row = {
                "Timestamp": data.get('timestamp', time.strftime('%Y-%m-%dT%H:%M:%SZ')),
                "Commodity": data['commodity'],
                "CurrentPrice": current_prices[data['commodity']],
                "Volume": data.get('volume', 0),
                "TradeID": data.get('trade_id', 0),
                "Quantity": data.get('quantity', 0),
                "EntryPrice": data.get('entry_price', 0),
                "Status": data.get('status', ''),
                "PositionType": data.get('position_type', ''),
                "ClosePrice": data.get('close_price', 0),
                "UnrealizedPnL": 0.0,
                "RealizedPnL": 0.0
            }

            if data['type'] == 'trade' and 'trade_id' in data:
                current_price = current_prices[data['commodity']]
                if data['status'] == 'Open':
                    row['UnrealizedPnL'] = calculate_pnl(data, current_price)
                elif data['status'] == 'Closed':
                    row['RealizedPnL'] = calculate_pnl(data, current_price, is_close=True)

            payload = [row]
            try:
                response = requests.post(PUSH_URL, headers=HEADERS, data=json.dumps(payload), timeout=10)
                if response.status_code != 200:
                    print(f"Push failed: {response.status_code} - {response.text}")
            except Exception as e:
                print(f"Request error: {e}")

        time.sleep(5)  # Control production rate

except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.close()
    consumer.close()