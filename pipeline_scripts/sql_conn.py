import pymssql
import configparser as cr
import sqlalchemy as alc


class GenConn:
    """We start by creating a connection to SQL Server, credentials are by default read from the
    configuration file cred.conf present in the folder 'config', to overwrite those values the 
    following keyword arguments can be passed during instantiation: user, pw, host, port, db_name"""
    
    def __init__(self, **conn_args):
        self.parser = cr.ConfigParser()
        self.parser.read('config/cred.conf')
        self.user = conn_args.get('user',self.parser.get("SQL_SERVER","user"))
        self.pw = conn_args.get('pw',self.parser.get("SQL_SERVER","password"))
        self.host = conn_args.get('host',self.parser.get("SQL_SERVER","host"))
        self.port = conn_args.get('port',self.parser.get("SQL_SERVER","port"))
        self.db_name = conn_args.get('db_name',self.parser.get("SQL_SERVER","database"))
    
    def create_eng(self):
        # Create SQL Alchemy engine for Pandas
        engine = alc.create_engine('mssql+pymssql://' + self.user + ':' + self.pw + '@' +
        self.host + ':' + self.port + '/' + self.db_name)
        return engine

    def create_conn(self):
        # Create a SQL connection with pymssql
        conn = pymssql.connect(server=self.host, port=self.port, user=self.user, 
        password=self.pw, database=self.db_name)
        return conn
