from confluent_kafka import Producer
import socket
import time
import json
from datetime import datetime
import random
import sys
from src.scripts.data_generator import create_user_data, create_transaction_data

no_msgs = int(sys.argv[1])
conf = {
    "bootstrap.servers": "localhost:9092",
    # "security.protocol": "SASL_SSL",
    # "sasl.mechanism": "PLAIN",
    #'sasl.username': '<CLUSTER_API_KEY>',
    #'sasl.password': '<CLUSTER_API_SECRET>',
    "client.id": socket.gethostname(),
}

producer = Producer(conf)
num_partitions = 3
# concurrency_list = [1,3,5]
input_interval = 1 # seconds delay

for _ in range(no_msgs):
    # concurrency = random.choice(concurrency_list)
    partition = random.choice(range(num_partitions))
    

    msg_dict = create_user_data(1) # You should get only one record at a time
    json_str = json.dumps(msg_dict[0])
    print(f"Partition :: {partition} :: {json_str}")
    # msg_key = random.randint(100,99999)
    
    # producer.produce(
    #     "OnlineUsersTopic",
    #     key=str(msg_key),
    #     value=json_str,
    #     partition=partition,        
    # )
    # producer.poll(1)
    time.sleep(input_interval)
