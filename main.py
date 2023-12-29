import pandas as pd
from unidecode  import unidecode
import pysolr

# def main():
#     df = pd.read_csv('aluno.csv')
#     df.columns=[col.lower() for col in df]
#     df.columns=[col.replace(" ", "_") for col in df]
#     df.columns=[unidecode(col) for col in df]
#     # breakpoint()
#     print('oi')
    
# main()




# Create a client instance. The timeout and authentication options are not required.
# solr = pysolr.Solr('http://localhost:8983/solr/', always_commit=True)

# Note that auto_commit defaults to False for performance. You can set
# `auto_commit=True` to have commands always update the index immediately, make
# an update call with `commit=True`, or use Solr's `autoCommit` / `commitWithin`
# to have your data be committed following a particular policy.

# solr.add([
#     {
#         "id": "doc_1",
#         "title": "A test document",
#     },
#     {
#         "id": "doc_2",
#         "title": "The Banana: Tasty or Dangerous?",
#         "_doc": [
#             { "id": "child_doc_1", "title": "peel" },
#             { "id": "child_doc_2", "title": "seed" },
#         ]
#     },
# ])



import json

solr_url = 'http://localhost:8983/solr/teste'
solr = pysolr.Solr(solr_url)

res = solr.ping()
response = json.loads(res)
if response.get('status') == 'OK':
    print('Connection to Solr server successful!')
else:
    print('Connection to Solr server failed.')
    print(response)

