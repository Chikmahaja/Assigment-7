from kafka import KafkaConsumer
import json
import ast

# Create consumer
consumer = KafkaConsumer('todos', bootstrap_servers=['localhost:9092'])
consumer.subscribe(['todos'])

# make list to save msg
saved_msg = []

# Wait for the data to be available in the topic

def main():
    while True:
        msg = consumer.poll(1.0) #timeout
        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        saved_msg = saved_msg.append(msg.value().decode('utf-8'))
        print(saved_msg)
    consumer.close()

# Convert all message from string to dictionary
saved_msg_copy = saved_msg.copy()
for count in range(len(saved_msg)):
    saved_msg_copy[count] = ast.literal_eval(saved_msg_copy[count])
saved_msg_copy

# Make The JSON structure
todos_data_consumer = {
    'todos': 
        saved_msg_copy
    }
todos_data_consumer

# Convert Dictionary to JSON
todos_json = json.dumps(todos_data_consumer, indent=4)

# Save JSON file
with open("todos.json", "w") as outfile:
    outfile.write(todos_json)
