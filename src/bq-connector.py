from google.cloud import bigquery

import sys.modules as modules

if 'bigquery' not in modules:
    from google.cloud import bigquery
if 'pd' not in modules:
    import pandas as pd

def bq_query(query, client, *args, **kwargs):
    return pd.DataFrame(client.query(query))