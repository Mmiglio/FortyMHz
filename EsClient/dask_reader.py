from dask.distributed import Client
from dask_elk.client import DaskElasticClient
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

def main():
    client = Client(n_workers=4, threads_per_worker=1)

    el_client = DaskElasticClient(host=['10.64.22.50:9200', '10.64.22.21:9200', '10.64.22.12:9200'])

    ddf = el_client.read(index='run-928', doc_type='_doc').persist()

    # filter events with low number of hits
    ddf = ddf[ddf['NHITS']>5]

    # convert dataframe to bag and create pandas dataframe from list of hits
    db = ddf['HITS_LIST'].to_bag().map(lambda hits_list: pd.DataFrame(hits_list))

    # show the dataframe relative to the first event
    print(db.take(1)[0].head())

    # done
    client.close()

if __name__ == "__main__":
    main()
