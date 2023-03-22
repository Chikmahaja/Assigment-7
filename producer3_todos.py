from kafka import KafkaProducer
import json
import requests

# Create producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Get product data from dummyAPI
resp = requests.get('https://dummyjson.com/todos?limit=7')
todos_dict = resp.json()

print(todos_dict["todos"])
data_todos = todos_dict["todos"]

for data in data_todos:
    print(data)
    
for val in data_todos:
    send_msg = str(val)

    # send data using the producer
    producer.send('todos', send_msg.encode('utf-8'))
    producer.flush()
