from confluent_kafka import Producer
import pandas as pd
import json
import sys

# read test data and prepare list of dicts
df = pd.read_csv('test_data.csv')
data = df.to_dict('records')

conf = {
    'bootstrap.servers': "kafka-cluster-kafka-bootstrap:9092"
}
producer = Producer(conf)

n_part = 4
curr_part = 0
for msg in data:
    try:
        producer.produce('topic4-real', value=json.dumps(msg).encode('utf-8'), partition=curr_part)
        curr_part = (curr_part+1)%n_part
    except BufferError:
        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                         len(producer))
        producer.flush()

producer.flush()