from dask.distributed import Client
from dask import bag as db
import time

def main():
    # create Dask client
    client = Client('127.0.0.1:8786')
    num_workers = len(client.scheduler_info()['workers'])

    # upload files used by workers
    client.upload_file('/home/user/dask_matteo/segment_reco_dummy.py')
    client.upload_file('/home/user/dask_matteo/dalek.py')
    from segment_reco_dummy import segments_reconstructor
    from dalek import Dalek

    # define an elastic query
    query = {
        'query': {
            'range': {
                'TIME_STAMP':{
                    "gte": "now-20d",
                    "lte": "now"
                }
            }
        }
    }

    # read data from elastic
    dalek = Dalek(el_hosts=['10.64.22.50', '10.64.22.21', '10.64.22.12', '10.64.22.60', '10.64.22.23'], num_workers=num_workers)
    delayed_results = dalek.read(index='run000937', query=query, size=num_workers*10)

    # create dask bag from results
    events_bag = db.from_delayed(delayed_results)

    # dummy reconstruction of events
    segments_bag = events_bag.map(lambda event: segments_reconstructor(
            hits_list=event['_source']['HITS_LIST'], run_id=event['_source']['RUN_ID']
        )
    )
    writing_res = dalek.write(segments_bag, doc_generator=documents_genetator)
    _ = writing_res.compute()

    print('Done.')

def documents_genetator(partition):
    for reco_obj in partition:
        # get segments and run id
        segments = reco_obj['segments']
        run_id = reco_obj['run_id']

        # create document
        doc = dict()
        doc['_op_type']: 'index'
        doc['_index'] = 'reco'+run_id
        doc['time_stamp'] = int(time.time()*1000)
        # reco_obj is a dictionary {chamber_x: np.poly1d}
        doc['reco_tracks'] = list()
        for chamber in segments:
            # reco tracks is of the type ax + b
            if segments[chamber] == None or len(segments[chamber].c)<2:
                continue 
            a, b = segments[chamber].c
            doc['reco_tracks'].append({
                'chamber': chamber,
                'a': a,
                'b': b
            })
        yield doc

if __name__ == "__main__":
    main()