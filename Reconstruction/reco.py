from dask.distributed import Client
from dask import bag as db

def main():
    # create Dask client
    client = Client('127.0.0.1:8786')
    num_workers = len(client.scheduler_info()['workers'])

    # upload files used by workers
    client.upload_file('/home/user/segment_reco_dummy.py')
    client.upload_file('/home/user/dask_matteo/reader.py')
    from segment_reco_dummy import segments_reconstructor
    from reader import Reader

    # define an elastic query
    query = {
        'query': {
            'range': {
                'TIME_STAMP':{
                    "gte": "now-5d",
                    "lte": "now"
                }
            }
        }
    }

    # read data from elastic
    reader = Reader(el_hosts=['10.64.22.50', '10.64.22.21', '10.64.22.12'], num_workers=num_workers)
    delayed_results = reader.read(index='run000937', query=query, size=num_workers*10)

    # create dask bag from results
    events_bag = db.from_delayed(delayed_results)

    # dummy reconstruction of events
    segments_bag = events_bag.map(lambda event: segments_reconstructor(event['_source']['HITS_LIST']))

    print(segments_bag.take(1))

if __name__ == "__main__":
    main()