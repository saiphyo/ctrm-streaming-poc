import json
import time
import random
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from confluent_kafka import Producer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("FABRIC_KAFKA_BOOTSTRAP")
CONNECTION_STRING = os.getenv("FABRIC_KAFKA_CONN_STR")
TOPIC = 'es_22fdb18a-51d8-4cc6-b304-2e16fb0545f1'

conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '$ConnectionString',
    'sasl.password': CONNECTION_STRING,
    'client.id': 'ctrm-producer'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print(f"Message failed: {err}")
    else:
        print(f"✓ Sent to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

commodities = ['Oil', 'Gold', 'Wheat']
current_prices = {comm: 100.0 for comm in commodities}
trade_counter = 0
open_trades = []

print("=" * 60)
print("CTRM KAFKA PRODUCER - WITH PRICE CHANGE DETECTION")
print("=" * 60)

try:
    while True:
        # UPDATE PRICES
        for comm in commodities:
            current_prices[comm] += random.uniform(-2, 2)
            current_prices[comm] = max(50, min(150, current_prices[comm]))

            # SEND PRICE TICK WITH LastUpdatedUtc
            payload = {
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commodity': comm,
                'CurrentPrice': round(current_prices[comm], 2),
                'Volume': round(random.uniform(1000, 5000), 2),
                'TradeID': 0,
                'Quantity': 0,
                'EntryPrice': 0,
                'Status': 'Tick',
                'PositionType': 'N/A',
                'ClosePrice': 0,
                'UnrealizedPnL': 0,
                'RealizedPnL': 0,
                'EventType': 'PriceTick',
                'LastUpdatedUtc': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            }
            # print(f"DEBUG: {json.dumps(payload, indent=2)}")  # Debug
            producer.produce(TOPIC, json.dumps(payload).encode('utf-8'), callback=delivery_report)

        # OPEN NEW TRADE (30% chance)
        if random.random() > 0.7:
            trade_counter += 1
            comm = random.choice(commodities)
            quantity = round(random.uniform(100, 1000), 2)
            entry_price = round(current_prices[comm], 2)
            position_type = random.choice(['Long', 'Short'])

            open_trades.append({
                'TradeID': trade_counter,
                'commodity': comm,
                'quantity': quantity,
                'entry_price': entry_price,
                'position_type': position_type,
                'open_time': time.time()
            })

            payload = {
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commodity': comm,
                'CurrentPrice': round(current_prices[comm], 2),
                'Volume': round(random.uniform(1000, 5000), 2),
                'TradeID': trade_counter,
                'Quantity': quantity,
                'EntryPrice': entry_price,
                'Status': 'Open',
                'PositionType': position_type,
                'ClosePrice': 0,
                'UnrealizedPnL': 0,
                'RealizedPnL': 0.0,
                'EventType': 'TradeOpened',
                'LastUpdatedUtc': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            }
            producer.produce(TOPIC, json.dumps(payload).encode('utf-8'), callback=delivery_report)
            print(f"📊 NEW TRADE: {position_type} {quantity} {comm} @ {entry_price}")

        # CLOSE TRADE (20% chance)
        if open_trades and random.random() > 0.8:
            trade_idx = random.randint(0, len(open_trades) - 1)
            open_trade = open_trades.pop(trade_idx)

            close_price = round(current_prices[open_trade['commodity']], 2)
            quantity = open_trade['quantity']
            entry_price = open_trade['entry_price']
            position_type = open_trade['position_type']

            if position_type == 'Long':
                realized_pnl = (close_price - entry_price) * quantity
            else:
                realized_pnl = (entry_price - close_price) * quantity

            payload = {
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'commodity': open_trade['commodity'],
                'CurrentPrice': round(current_prices[open_trade['commodity']], 2),
                'Volume': round(random.uniform(1000, 5000), 2),
                'TradeID': open_trade['TradeID'],
                'Quantity': quantity,
                'EntryPrice': entry_price,
                'Status': 'Closed',
                'PositionType': position_type,
                'ClosePrice': close_price,
                'UnrealizedPnL': 0,
                'RealizedPnL': round(realized_pnl, 2),
                'EventType': 'TradeClosed',
                'LastUpdatedUtc': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            }
            producer.produce(TOPIC, json.dumps(payload).encode('utf-8'), callback=delivery_report)
            print(f"✓ TRADE CLOSED: {open_trade['TradeID']} | PnL: {realized_pnl:.2f}")

        # UPDATE UNREALIZED PnL (50% of open trades)
        for trade in open_trades:
            if random.random() > 0.5:
                price = current_prices[trade['commodity']]
                if trade['position_type'] == 'Long':
                    unrealized_pnl = (price - trade['entry_price']) * trade['quantity']
                else:
                    unrealized_pnl = (trade['entry_price'] - price) * trade['quantity']

                payload = {
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'commodity': trade['commodity'],
                    'CurrentPrice': round(price, 2),
                    'Volume': round(random.uniform(1000, 5000), 2),
                    'TradeID': trade['TradeID'],
                    'Quantity': trade['quantity'],
                    'EntryPrice': trade['entry_price'],
                    'Status': 'Open',
                    'PositionType': trade['position_type'],
                    'ClosePrice': 0,
                    'UnrealizedPnL': round(unrealized_pnl, 2),
                    'RealizedPnL': 0.0,
                    'EventType': 'PriceTick',
                    'LastUpdatedUtc': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                }
                producer.produce(TOPIC, json.dumps(payload).encode('utf-8'), callback=delivery_report)

        time.sleep(5)
        print()

except KeyboardInterrupt:
    print("\n⚠ Producer stopped")
finally:
    producer.flush()
    print("✓ Flushed")