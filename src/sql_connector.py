import sys.modules as modules

if 'pd' not in modules:
    import pandas as pd
#wrapper around pd.read_sql
def sql_query(query, conn, *args, **kwargs):
    return pd.read_sql(query, conn, *args, **kwargs)