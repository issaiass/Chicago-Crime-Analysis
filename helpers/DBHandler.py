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

class UsersDB:
    def __init__(self, name='credentials.db'):
        self.name = name
        self.tablename = 'Users'
        self.dbpath = os.path.join(os.getcwd(), 'database', self.name) 

    def create_db(self):
        conn = sqlite3.connect(self.dbpath)
        cur = conn.cursor()
 
        # Create table
        cur.executescript('''
            CREATE TABLE IF NOT EXISTS Users (
            USER     TEXT NOT NULL PRIMARY KEY UNIQUE,
            PUBLIC_ID TEXT,
            PASSWORD TEXT,
            ADMIN BOOLEAN);''')
        cur.close()
        conn.close()

    def insert_user(self, user, password, public_id, admin):
        print('[INFO] SQL inserting user and password')        
        tablename = self.tablename
        conn = sqlite3.connect(self.dbpath)
        cur = conn.cursor()
        cur.execute('''INSERT INTO Users(user,password,public_id,admin) VALUES(?,?,?,?)
                       ON CONFLICT(user) DO 
                       UPDATE SET user=?,password=?,public_id=?,admin=?''', 
                       (str(user), str(password), str(public_id), str(admin),
                        str(user), str(password), str(public_id), str(admin)))
        conn.commit()
        conn.close()
        print('[INFO] SQL user and password inserted')

    def delete_user(self, public_id):
        print('[INFO] SQL deleting a user')        
        conn = sqlite3.connect(self.dbpath)
        cur = conn.cursor()
        cur.execute('''DELETE FROM Users WHERE public_id=?''', (str(public_id),))
        conn.commit()
        conn.close()
        print('[INFO] SQL user deleted')

    def get_user_info(self, public_id):
        print('[INFO] SQL getting the user info')        
        conn = sqlite3.connect(self.dbpath)
        cur = conn.cursor()
        cur.execute('''SELECT user,password FROM Users WHERE public_id=?''', (str(public_id),))
        print('[INFO] SQL finish the user info')        
        return cur

    def get_all_users_info(self):
        print('[INFO] SQL getting the users info')        
        conn = sqlite3.connect(self.dbpath)
        cur = conn.cursor()
        cur.execute('''SELECT * FROM Users''')
        print('[INFO] SQL finish the users info')        
        return cur        

    def select_by(self, filter, value):
        print('[INFO] SQL select statement')        
        conn = sqlite3.connect(self.dbpath)
        cur = conn.cursor()
        cur.execute('''SELECT * FROM Users WHERE ''' + filter + '''=?''', (str(value),))
        print('[INFO] SQL finish the select')        
        return cur        

if __name__ == '__main__':
    generate_historical_db = False
    generate_users_db = True

    if generate_historical_db:
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
    if generate_users_db:
        usersdb = UsersDB()
        usersdb.create_db()
        usersdb.insert_user('admin', generate_password_hash('admin', method='sha256'), uuid.uuid4(), True) # insert or replace
        uuid_rangel = uuid.uuid4()
        usersdb.insert_user('rangel', generate_password_hash('kxXoa1.11', method='sha256'), uuid_rangel, False) # insert or replace
        usersdb.insert_user('rangel', generate_password_hash('alvarado', method='sha256'), uuid_rangel, False) # insert or replace
        uuid_roberto = uuid.uuid4()
        usersdb.insert_user('roberto', generate_password_hash('hassell', method='sha256'), uuid_roberto, False) # insert or replace
        uuid_mambo = uuid.uuid4()
        usersdb.insert_user('mambo', generate_password_hash('Ab)l9v', method='sha256'), uuid_mambo, False) # insert or replace    
        usersdb.delete_user(uuid_mambo) # delete
        users = usersdb.get_user_info(uuid_rangel) # one user
        info = users.fetchone() # one user
        print(info)
        users = usersdb.get_all_users_info() # all users    
        for one_user_info in users:
            print(one_user_info)
        user = usersdb.select_by('user', 'roberto')
        user = user.fetchone()
        print(user)