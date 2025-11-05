import json
import time
import random
import os
from dotenv import load_dotenv
load_dotenv()
from confluent_kafka import Producer

# === FABRIC KAFKA ENDPOINT (from Custom App source) ===
BOOTSTRAP_SERVERS = os.getenv("FABRIC_BOOTSTRAP")      # e.g. ctrm-rti-poc-eh.servicebus.windows.net:9093
CONNECTION_STRING = os.getenv("FABRIC_CONN_STR")
TOPIC = 'es_e4bdbac5-7a11-43ee-ba83-ccef8856cf54'


# SASL config
conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '$ConnectionString',
    'sasl.password': CONNECTION_STRING,
    'client.id': 'ctrm-producer'
}

producer = Producer(conf)

# Delivery callback (optional, for confirmation)
def delivery_report(err, msg):
    if err:
        print(f"Message failed: {err}")
    else:
        print(f"Sent to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

# === STATE ===
commodities = ['Oil', 'Gold', 'Wheat']
current_prices = {comm: 100.0 for comm in commodities}
trade_counter = 0
open_trades = {}

def calculate_pnl(trade, current_price, is_close=False):
    """Calculate PnL for a trade.

    This helper accepts trade dicts with different key naming conventions (e.g.,
    lowercase keys used when creating ephemeral dicts and CamelCase keys used in
    produced payloads). It will try common variants for each required field.
    """
    def _get(*names):
        for n in names:
            if n in trade:
                return trade[n]
        return None

    position = _get('position_type', 'PositionType', 'positionType')
    entry_price = _get('entry_price', 'EntryPrice', 'entryPrice')
    close_price = _get('close_price', 'ClosePrice', 'closePrice')
    quantity = _get('quantity', 'Quantity', 'qty', 'Qty')

    # Fallbacks / basic validation
    if position is None or entry_price is None or quantity is None:
        raise KeyError(f"Missing required trade fields for PnL calculation: position={position}, entry={entry_price}, qty={quantity}")

    # Use current_price for unrealized PnL, and close_price when closing
    if is_close:
        if close_price is None:
            # If close price not present, assume provided current_price is the close
            close_price = current_price

    if position == 'Long':
        if not is_close:
            diff = (current_price - entry_price)
        else:
            diff = (close_price - entry_price)
    else:
        if not is_close:
            diff = (entry_price - current_price)
        else:
            diff = (entry_price - close_price)

    return round(diff * quantity, 2)

print("CTRM Producer → Fabric Eventstream (confluent-kafka)")

try:
    # Initial prices
    for comm in commodities:
        payload = {
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'commodity': comm,
            'CurrentPrice': current_prices[comm],
            'Volume': round(random.uniform(1000, 5000), 2),
            'TradeID': 0,
            'Quantity': 0,
            'EntryPrice': 0,
            'Status': '',
            'PositionType': '',
            'ClosePrice': 0,
            'UnrealizedPnL': 0.0,
            'RealizedPnL': 0.0
        }
        producer.produce(TOPIC, json.dumps(payload).encode('utf-8'), callback=delivery_report)

    while True:
        # Flush any pending messages
        producer.poll(0)

        # Produce prices
        for comm in commodities:
            current_prices[comm] += random.uniform(-5, 5)
            payload = {
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commodity': comm,
                'CurrentPrice': round(current_prices[comm], 2),
                'Volume': round(random.uniform(1000, 5000), 2),
                'TradeID': 0,
                'Quantity': 0,
                'EntryPrice': 0,
                'Status': '',
                'PositionType': '',
                'ClosePrice': 0,
                'UnrealizedPnL': 0.0,
                'RealizedPnL': 0.0
            }
            producer.produce(TOPIC, json.dumps(payload).encode('utf-8'), callback=delivery_report)

        # New trade
        if random.random() > 0.7:
            trade_counter += 1
            comm = random.choice(commodities)
            quantity = round(random.uniform(100, 1000), 2)
            entry_price = round(current_prices[comm] + random.uniform(-10, 10), 2)
            position_type = random.choice(['Long', 'Short'])
            unrealized_pnl = calculate_pnl({'position_type': position_type, 'entry_price': entry_price, 'quantity': quantity}, current_prices[comm])

            payload = {
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commodity': comm,
                'CurrentPrice': current_prices[comm],
                'Volume': 0,
                'TradeID': trade_counter,
                'Quantity': quantity,
                'EntryPrice': entry_price,
                'Status': 'Open',
                'PositionType': position_type,
                'ClosePrice': 0,
                'UnrealizedPnL': unrealized_pnl,
                'RealizedPnL': 0.0
            }
            producer.produce(TOPIC, json.dumps(payload).encode('utf-8'), callback=delivery_report)
            open_trades[trade_counter] = payload

        # Close trade
        if open_trades and random.random() > 0.8:
            close_id = random.choice(list(open_trades.keys()))
            trade = open_trades.pop(close_id)
            trade['Status'] = 'Closed'
            trade['ClosePrice'] = round(current_prices[trade['commodity']], 2)
            trade['RealizedPnL'] = calculate_pnl(trade, current_prices[trade['commodity']], is_close=True)
            trade['UnrealizedPnL'] = 0.0
            trade['timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%SZ')
            producer.produce(TOPIC, json.dumps(trade).encode('utf-8'), callback=delivery_report)

        producer.flush()  # Ensure all messages are sent
        time.sleep(5)

except KeyboardInterrupt:
    print("\nStopping...")
finally:
    producer.flush()