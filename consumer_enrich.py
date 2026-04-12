from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    print(message)
    
    tx = message.value
    amount = tx.get("amount", 0)
    
    if amount > 3000:
        tx['risk_level'] = "HIGH"
    elif amount > 1000:
        tx['risk_level'] = "MEDIUM"
    else:
        tx['risk_level'] = "LOW"
    print(f"ID: {tx['tx_id']} | Kwota: {amount:7.2f} | RYZYKO: {tx['risk_level']}")