import sys.modules as modules

if 'bigquery' not in modules:
    from google.cloud import bigquery
if 'pd' not in modules:
    import pandas as pd

def bq_query(client, query,  *args, **kwargs):
    return client.query(query, *args, **kwargs).to_dataframe()