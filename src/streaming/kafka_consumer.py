from confluent_kafka import Consumer, KafkaException, KafkaError
import sys

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "foo",
    "auto.offset.reset": "smallest",
}

consumer = Consumer(conf)

running = True


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write(
                        "%% %s [%d] reached end at offset %d\n"
                        % (msg.topic(), msg.partition(), msg.offset())
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print("Printing kafka mssg key and value")
                print(msg.key(), msg.value(), msg.partition())
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


basic_consume_loop(consumer=consumer, topics=["OnlineUsersTopic"])
