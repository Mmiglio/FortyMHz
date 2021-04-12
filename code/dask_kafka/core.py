import confluent_kafka as ck
from dask.distributed import get_worker

import json

class KafkaReader(object):
    def __init__(
        self,
        client,
        topic,
        consumer_conf,
        batch_size=10000
        ):

        self.client = client

        self.topic = topic
        self.conf = consumer_conf

        self.batch_size = batch_size

        # create master consumer and set/overwrite conf
        self.conf['auto.offset.reset'] = 'earliest'
        self.conf['enable.auto.commit'] = 'false'
        self.master_consumer = ck.Consumer(self.conf)

    def read_batch(self):
        """
        TODO: describe func
        """

        # get partitions metadata
        parts_metadata = self.master_consumer \
            .list_topics(self.topic) \
            .topics[self.topic].partitions

        # create a list of partitions to be processed
        partitions_list = []
        for part_num in range(len(parts_metadata)):
            # create a TopicPartion object for the current partition
            tp = ck.TopicPartition(self.topic, part_num)

            # get first and last offsets for the current partition
            low, high = self.master_consumer.get_watermark_offsets(tp)

            # get the last committed position (take the first element)
            committed_offset = self.master_consumer.committed([tp], timeout=1)[0].offset
            if committed_offset==-1001:
                committed_offset = 0

            # check if there are new messages
            if low + committed_offset == high:
                print(f"No new messages in partition {part_num}.")
                continue
            else:
                partitions_list.append((part_num, low+committed_offset, high))

        if len(partitions_list)==0:
            print("All partitions have been processed. Skipping.")
            return []
        else:
            # send the partitions to the workers
            clients_fut = self.client.scatter(partitions_list, broadcast=True)

            # read the partition in each worker
            partitions_fut = [
                self.client.submit(
                    _read_partition, fut, self.topic, self.conf, self.batch_size
                ) for fut in clients_fut
            ]

            # return the list of futures
            return partitions_fut

    def close_consumers(self):
        """
        TODO: describe func
        """        
        res = self.client.run(_close_consumers)
        for r in res:
            print(r) 
            

def _read_partition(part_metadata, topic, conf, batch_size):
    """
    TODO: describe func
    """
    print("Here1")
    part_no, low, high = part_metadata

    tp = ck.TopicPartition(topic, part_no, low)

    # create the consumer only the first time it is called
    # then store it in the worker state dict
    worker_state = get_worker() 
    if not hasattr(worker_state, 'consumer'):
        worker_state.consumer = None

    # first call -> create consumer
    if worker_state.consumer == None:
        worker_state.consumer = ck.Consumer(conf)

    # use the consumer stored in the worker dict
    c = worker_state.consumer

    last_offset = low
    c.assign([tp])
    
    print("Created consumer")
    # get a batch of messages
    messages = c.consume(min(batch_size, high - last_offset))
    print("Read messages")
    values = []
    for m in messages:
        last_offset = m.offset()
        values.append(json.loads(m.value().decode('utf-8')))

    # commit the current offset
    _tp = ck.TopicPartition(topic, part_no, last_offset+1)
    c.commit(offsets=[_tp], asynchronous=True)

    return values

def _close_consumers():
    worker_state = get_worker()  
    if not hasattr(worker_state, 'consumer'):
        return "No consumer, skipping."
    else:
        c = worker_state.consumer
        c.close()
        delattr(worker_state, 'consumer')
        return "Closed consumer."
