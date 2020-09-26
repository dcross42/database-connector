import sys.modules as modules

if 'pd' not in modules:
    import pandas as pd

def sql_query(conn, query, *args, **kwargs):
    return pd.read_sql(query, conn, *args, **kwargs)