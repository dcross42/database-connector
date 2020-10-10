import sys.modules as modules

if 'pymongo' not in modules:
    import pymongo

if 'pd' not in modules:
    import pandas as pd

def mongo_query(conn, query, col, *args, **kwargs):
    collection = conn[col]
    return pd.read_json(collection.find(query))