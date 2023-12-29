import pandas as pd
from unidecode  import unidecode
import json
import pysolr
import requests

pysolr.SolrCoreAdmin()


def main():
    df = pd.read_csv('aluno.csv')
    df.columns=[col.lower() for col in df]
    df.columns=[col.replace(" ", "_") for col in df]
    df.columns=[unidecode(col) for col in df]
    # breakpoint()
    print('oi')

core_name = 'aluno'

# create core    
requests.post(f'http://localhost:8983/solr/admin/cores?action=CREATE&name={core_name}&instanceDir={core_name}')

breakpoint()

# Create a client instance. The timeout and authentication options are not required.
solr = pysolr.Solr(f'http://localhost:8983/solr/{core_name}')


res = solr.ping()
response = json.loads(res)
if response.get('status') != 'OK':
    print(res)
    raise ConnectionError('could not ping the core. please check log above')
