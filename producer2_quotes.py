from kafka import KafkaProducer
import json
import requests

# Create producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Get product data from dummyAPI
resp = requests.get('https://dummyjson.com/quotes?limit=7')
quotes_dict = resp.json()

print(quotes_dict["quotes"])
data_quotes = quotes_dict["quotes"]

for data in data_quotes:
    print(data)
    
for val in data_quotes:
    send_msg = str(val)

    # send data using the producer
    producer.send('quotes', send_msg.encode('utf-8'))
    producer.flush()
