from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json
import requests

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='ml-scoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

API_URL = "http://localhost:8001/score"

print("Serwis scoringowy ML uruchomiony...")

for message in consumer:
    tx = message.value
    is_electronics = 1 if tx.get('category') == 'elektronika' else 0
    features = {
        "amount": float(tx.get('amount', 0)),
        "is_electronics": int(is_electronics),
        "tx_per_minute": int(tx.get('tx_per_minute', 5)) # Domyślnie 5, jeśli brak w danych
    }

    try:
        response = requests.post(API_URL, json=features)
        prediction = response.json()
        if prediction.get("is_fraud"):
            alert_data = {
                "timestamp": datetime.now().isoformat(),
                "tx_id": tx.get("tx_id"),
                "fraud_probability": prediction.get("fraud_probability"),
                "original_data": tx
            }
            alert_producer.send('alerts', value=alert_data)
            print(f"!!! ALERT !!! Wykryto oszustwo! ID: {tx.get('tx_id')} | "
                  f"Prawdopodobieństwo: {prediction.get('fraud_probability'):.2%}")
    except Exception as e:
        print(f"Błąd podczas scoringu transakcji {tx.get('tx_id')}: {e}")

alert_producer.flush()
