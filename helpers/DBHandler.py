# import the necessary packages
import sqlite3
import pandas as pd
import os
import datetime
from werkzeug.security import generate_password_hash
import uuid

class DBHandler:
    def __init__(self,name='preditiva.db', csv='dataset.csv'):
        self.name = name
        self.csv  = csv
        self.tablename = 'History'
        self.dbpath = os.path.join('database', self.name) 

    def drop_table(self, tablename='History'):
        conn = sqlite3.connect(self.dbpath)
        cur = conn.cursor()
        cur.executescript('''DROP TABLE IF EXISTS ''' + tablename + ''';''')
        cur.close()
        conn.close()        

    def create_db(self):
        conn = sqlite3.connect(self.dbpath)
        cur = conn.cursor()

       # Create table
        cur.executescript('''
            CREATE TABLE IF NOT EXISTS ''' + self.tablename + ''' (
            ID     INTEGER NOT NULL PRIMARY KEY UNIQUE,
            DATE   DATETIME,
            BLOCK TEXT,
            CASE_NUM TEXT,
            DESCRIPTION TEXT,
            IUCR TEXT,
            PRIMARY_TYPE TEXT,
            LOCATION_DESCRIPTION TEXT,
            ARREST BOOLEAN,
            DOMESTIC BOOLEAN,
            BEAT INTEGER,
            DISTRICT INTEGER,
            WARD INTEGER,
            COMMUNITY_AREA INTEGER,
            FBI_CODE INTEGER,
            X_COORDINATE REAL,
            Y_COORDINATE REAL,
            YEAR INTEGER,
            UPDATED_ON DATETIME,
            LATITUDE REAL,
            LONGITUDE REAL,
            LOCATION TEXT
            );
        ''')
        cur.close()
        conn.close()

    def insert_csv(self, csv_file):
        print('[INFO] SQL insert process starting')        
        tablename = self.tablename
        conn = sqlite3.connect(self.dbpath)
        df = pd.read_csv(csv_file, parse_dates=['Date'])
        columns = [col.upper().replace(' ', '_') for col in df.columns]   
        df.columns = columns   
        df.to_sql(tablename, con=conn, if_exists='replace', index=False)
        conn.commit()
        conn.close()
        print('[INFO] SQL insert process finished')        

    def insert_json(self, json_obj):
        pass

    def select(self, start='2001-01-01 00:00:00', end='2001-01-01 00:00:01'):
        conn = sqlite3.connect(self.dbpath)
        cur = conn.cursor()
        sql_query = '''SELECT PRIMARY_TYPE, LATITUDE, LONGITUDE, DATE FROM History
                       WHERE DATE >= datetime(?) 
                       AND DATE <= datetime(?)'''
        cur.execute(sql_query, [start, end])
        return cur


if __name__ == '__main__':
    dbh = DBHandler()
    dbh.drop_table()
    dbh.create_db()
    dbh.insert_csv('dataset.csv')
    # EXAMPLE QUERY
    # SELECT PRIMARY_TYPE, LATITUDE, LONGITUDE FROM History WHERE DATE >= '2006-04-02 13:00:00' 
    # AND History.DATE < '2006-04-03 00:00:00'
    cursor = dbh.select(start="2006-04-02 13:00:00", end="2006-04-03 00:00:00")
    for row in cursor:
        print(row)