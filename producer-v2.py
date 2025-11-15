import json
import time
import random
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from azure.kusto.data import KustoConnectionStringBuilder, DataFormat
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, StreamDescriptor
import io

load_dotenv()

# Eventhouse Configuration
EVENTHOUSE_QUERY_URI = os.getenv("EVENTHOUSE_QUERY_URI")  # e.g., "https://your-eventhouse.z22.kusto.fabric.microsoft.com"
EVENTHOUSE_INGEST_URI = os.getenv("EVENTHOUSE_INGEST_URI")  # e.g., "https://ingest-your-eventhouse.z22.kusto.fabric.microsoft.com"
EVENTHOUSE_DATABASE = os.getenv("EVENTHOUSE_DATABASE")  # Your database name
EVENTHOUSE_TABLE = os.getenv("EVENTHOUSE_TABLE", "CTRMTrades")  # Table name

# Authentication - Using interactive login (you can also use service principal or managed identity)
kcsb_ingest = KustoConnectionStringBuilder.with_interactive_login(EVENTHOUSE_INGEST_URI)

# Create ingestion client
ingest_client = QueuedIngestClient(kcsb_ingest)

# Define ingestion properties
ingestion_properties = IngestionProperties(
    database=EVENTHOUSE_DATABASE,
    table=EVENTHOUSE_TABLE,
    data_format=DataFormat.JSON
)

def ingest_event(payload):
    """Send single event to Eventhouse"""
    try:
        # Convert payload to JSON string
        json_data = json.dumps(payload)

        # Create stream from JSON data
        data_stream = io.StringIO(json_data)

        # Create stream descriptor
        stream_descriptor = StreamDescriptor(data_stream, is_compressed=False, size=len(json_data))

        # Ingest from stream
        ingest_client.ingest_from_stream(stream_descriptor, ingestion_properties=ingestion_properties)

        print(f"✓ Ingested: {payload.get('EventType')} - {payload.get('commodity')} - TradeID: {payload.get('TradeID')}")
    except Exception as e:
        print(f"✗ Ingestion failed: {e}")

# Business Logic - Same as producer-v1.py
commodities = ['Oil', 'Gold', 'Wheat']
current_prices = {comm: 100.0 for comm in commodities}
trade_counter = 0
open_trades = []

print("=" * 60)
print("CTRM EVENTHOUSE PRODUCER - DIRECT INGESTION")
print("=" * 60)

try:
    while True:
        # UPDATE PRICES
        for comm in commodities:
            current_prices[comm] += random.uniform(-2, 2)
            current_prices[comm] = max(50, min(150, current_prices[comm]))

            # SEND PRICE TICK
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
            ingest_event(payload)

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
            ingest_event(payload)
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
            ingest_event(payload)
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
                ingest_event(payload)

        time.sleep(5)
        print()

except KeyboardInterrupt:
    print("\n⚠ Producer stopped")
finally:
    print("✓ Shutdown complete")