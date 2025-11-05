# CTRM Real-Time Power BI POC (Streaming Dataset)

## Quick Start

1. Open in VS Code: code ctrm-streaming-poc
2. Start Kafka: docker-compose up -d
3. Create topic (in kafka container): /opt/kafka/bin/kafka-topics.sh --create --topic ctrm-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
4. Install deps: pip install -r requirements.txt
5. Edit producer.py: Add your Power BI Push URL
6. Run: python producer.py

Dashboard: Create streaming push dataset in Power BI Service, use the schema from our earlier guide.
