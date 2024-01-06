import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime as dt, timedelta

con = None
cursor = None

try:
    con = mysql.connector.connect(
        host='172.24.144.1',
        user='admin',
        password='admin',
        database='real_estate_request'
    )
    if con.is_connected():
        print("Database connected!")
        cursor = con.cursor()
        csv_path = '/home/leminhphuc/real_estate_data/bds123.csv'
        df = pd.read_csv(csv_path)

        now = dt.now() - timedelta(days=365)

        select_query = "Delete FROM user_request where date_time < %s"
        cursor.execute(select_query, (now,))
        con.commit()
        print("Data deleted successfully!")
except Error as e:
    print(f"Error: {e}")
finally:
    # Close the database connection and cursor
    if cursor:
        cursor.close()
    if con and con.is_connected():
        con.close()
        print("MySQL connection is closed.")