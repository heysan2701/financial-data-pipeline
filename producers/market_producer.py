import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Initialize Kafka Producer pointing to the port exposed in docker-compose
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_nav_data():
    """Simulates real-time AMFI Mutual Fund NAV updates."""
    funds = [
        {"scheme_code": 119551, "scheme_name": "HDFC Top 100 Fund - Growth"},
        {"scheme_code": 103175, "scheme_name": "SBI Bluechip Fund - Growth"},
        {"scheme_code": 147714, "scheme_name": "Parag Parikh Flexi Cap Fund - Growth"}
    ]
    selected_fund = random.choice(funds)
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scheme_code": selected_fund["scheme_code"],
        "scheme_name": selected_fund["scheme_name"],
        "nav": round(random.uniform(50.0, 550.0), 4)
    }

def generate_transaction_data():
    """Simulates user investment transactions (NSE/BSE execution mock)."""
    return {
        "transaction_id": f"TXN_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scheme_code": random.choice([119551, 103175, 147714]),
        "transaction_type": random.choice(["BUY", "SELL"]),
        "amount": round(random.uniform(500.0, 50000.0), 2),
        "status": "SUCCESS"
    }

if __name__ == "__main__":
    print("🚀 Kafka Financial Data Producer started streaming...")
    try:
        while True:
            # 1. Produce NAV data to 'nav_raw' topic
            nav_payload = generate_nav_data()
            producer.send('nav_raw', value=nav_payload)
            print(f"Sent NAV Data -> {nav_payload['scheme_name']}: {nav_payload['nav']}")
            
            # 2. Produce Transaction data to 'txn_raw' topic
            txn_payload = generate_transaction_data()
            producer.send('txn_raw', value=txn_payload)
            print(f"Sent Transaction -> {txn_payload['transaction_id']} | {txn_payload['transaction_type']}")
            
            # Wait 3 seconds before streaming the next batch
            time.sleep(3)
            
    except KeyboardInterrupt:
        print("\n🛑 Streaming stopped by user.")
    finally:
        producer.flush()
        producer.close()