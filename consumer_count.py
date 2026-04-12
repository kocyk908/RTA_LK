from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

# TWÓJ KOD
# Dla każdej wiadomości:
#   1. store_counts[store] += 1
#   2. total_amount[store] += amount
#   3. Co 10 wiadomości: print tabela

for message in consumer:
    tx = message.value
    store = tx.get('store')
    amount = tx.get('amount', 0)
    
    store_counts[store] += 1 # 1.
    total_amount[store] += amount # 2.
    
    msg_count += 1
    if msg_count % 10 == 0: 
        print(f"\n--- Podsumowanie po {msg_count} wiadomosciach ---")
        print(f"{'SKLEP':<15} | {'LICZBA TX':<10} | {'SUMA PLN':<12}")
        print("-" * 45) # 3.
        
        for s in store_counts:
            print(f"{s:<15} | {store_counts[s]:<10} | {total_amount[s]:>10.2f}")
        print("-" * 45)
