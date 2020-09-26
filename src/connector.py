#This class is a superclass for different connectors
#Reads configuration information and connects to a server
class DBConnector():

    def __init__(self, service_type, config_file, key, *args, **kwargs):
        self.service = service_type
        self.conn = create_connection(service_type, config_file, key, *args, **kwargs)
    
    #query the database and return a pandas df
    def query():
        return

    def create_connection(self, config_file, key=None, *args, **kwargs):
        if(service_type.contains('sql') or service_type == 'oracle'):
            from configparser import ConfigParser
            from sqlalchemy import create_engine
            db_type, user, password, host, port, database = read_sql_config(creds, key)
            if(db_type == 'sqlite'):
                conn_string = f'{db_type}:///{user}:{password}@{host}:{port}/{database}'
            elif (db_type in ['mysql', 'postgresql', 'mssql', 'oracle']):
                conn_string = f'{db_type}://{user}:{password}@{host}:{port}/{database}'
            else:
                raise KeyError('Invalid SQL database type')
            try:
                conn = create_engine(conn_string, **kwargs)
                return conn
            except Exception as ex:
                print(ex)
        elif (service_type == 'mongodb'):
            from configparser import ConfigParser
            from pymongo import MongoClient
            db_type, user, password, host, port, database = read_mongo_config(creds, key)
            conn_string = f'{db_type}://{host}:{port}'
            client = MongoClient(conn_string)
            return client[database] #return database connection
        elif (service_type == 'bigquery'):
            from google.cloud import bigquery
            from google_auth_oauthlib import flow
            #based on Google BigQuery authentication documentation
            appFlow = flow.InstalledAppFlow.from_client_secrets_file(config_file, scopes=['https://www.googleapis.com/auth/bigquery'])
            appFlow.run_console()
            creds = appFlow.credentials
            if(key is None):
                raise ValueError('No BigQuery project specified')
            client = bigquery.Client(project=key, credentials = creds)
            return client #not the exact same as a connection, but it serves similar function
        else:
            raise ValueError('Invalid Service')

    def read_sql_config(config_file, key):
        config = ConfigParser()
        config.read(config_file)

        creds = config[key]

        db_type = creds['db_type']
        user = creds['user']
        password = creds['password']
        host = creds['host']
        port = creds['port']
        database = creds['database']
        return db_type, user, password, host, port, database
    
    def read_mongo_config(config_file, key):
        config = ConfigParser()
        config.read(config_file)

        creds = config[key]

        db_type = creds['db_type']
        host = creds['host']
        port = creds['port']
        database = creds['database']
        return db_type, host, port, database