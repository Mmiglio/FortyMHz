from elasticsearch import Elasticsearch
import dask

class Reader(object):
    def __init__(self, num_workers, el_hosts):
        self.el_hosts = el_hosts
        self.num_workers = num_workers

    @dask.delayed
    def worker_reader(self, index, query, worker_range):
        # format elastic request body
        body = {
            'from': worker_range[0], 'size': worker_range[1],
            'query': query['query']
        }

        # create elastic client
        elastic_client = Elasticsearch(self.el_hosts)
        delayed_result = elastic_client.search(index=index, body=body)
        # return delayed list of events 
        return delayed_result['hits']['hits']

    def read(self, index, query, size):
        # generate a list of touples (from, size) used by worker to read the examples
        partition_size = size // self.num_workers
        if partition_size > 10000:
            raise Exception('Partion size per worker (size//num_workers) must be less than 10000')
        ranges = [(i*partition_size, partition_size) for i in range(self.num_workers)]
        futures = list()
        for worker_range in ranges:
            futures.append(self.worker_reader(index, query, worker_range))
        
        # return list of futures
        return futures
    