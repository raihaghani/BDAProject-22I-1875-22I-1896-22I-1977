from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'user_actions',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def consume_user_actions():
    for message in consumer:
        process_action(message.value)

def process_action(action):
    print(f"Received action {action['action']} from user {action['user_id']}")
    # Logic to generate recommendations based on the action
