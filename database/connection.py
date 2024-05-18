import pyodbc
import pandas as pd

class SQl_Server_Connection:
    def __init__(self, dbname, user, password, host='localhost'):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.conn = None

    def connect(self):
        try:
            self.connection = pyodbc.connect(
                f"""DRIVER={'SQL Server'};
                    SERVER={self.host};
                    DATABASE={self.dbname};
                    UID={self.user};
                    PWD={self.password};"""
                )            
            print("Connection started!")
        except pyodbc.Error as e:
            print("Erro ao conectar o SQL Server:", e)

    def get_persons(self, query):
        if not self.connection:
            print("Not connected")
            return None
        try:
            df = pd.read_sql(query, self.connection)
            return df
        except pyodbc as e:
            print("Erro ao executar a consulta", e)
            return None

    def close(self):
        if self.connection:
            self.connection.close()
            print("Connection closed.")


    