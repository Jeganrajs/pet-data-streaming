from confluent_kafka import Producer
import socket
import time
import json
from datetime import datetime
import random
import sys
# from scripts.data_generator import create_transaction_data
from src.scripts.trans_data_generator import TransactionFakerModel

# >> python   /mnt/d/jegan/git_repos/pet-data-streaming/src/scripts/kafka_transaction_producer.py 100
no_msgs = int(sys.argv[1])
# no_msgs = 3
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
# input_interval = 1 # seconds delay

for _ in range(no_msgs):
    # concurrency = random.choice(concurrency_list)
    partition = random.choice(range(num_partitions))
    

    # Create transaction faker model
    transaction_faker = TransactionFakerModel()
    
    # Generate transactions
    msg_dict = transaction_faker.generate_transactions(1)
    # print(msg_dict)

    json_str = json.dumps(msg_dict[0])
    print(json_str)

    # print(f"Partition :: {partition} :: {json_str}")
    msg_key = random.randint(100,99999)
    
    producer.produce(
        "OnlineTransactionsTopic",
        key=str(msg_key),
        value=json_str,
        partition=partition,        
    )
    producer.poll(1)
    time.sleep(random.randint(0,1))
