from kafka import KafkaProducer
import json
import requests

# Create producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Get product data from dummyAPI
resp = requests.get('https://dummyjson.com/products?limit=7')
products_dict = resp.json()

print(products_dict["products"])
data_products = products_dict["products"]

for data in data_products:
    print(data)
    
for val in data_products:
    send_msg = str(val)

    # send data using the producer
    producer.send('products', send_msg.encode('utf-8'))
    producer.flush()
