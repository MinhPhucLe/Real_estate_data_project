import mysql.connector
from mysql.connector import Error
import pandas as pd

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
        csv_path = '/home/leminhphuc/real_estate_data/cenhomes.csv'
        df = pd.read_csv(csv_path)

        table_name = 'real_estate'
        df = df.fillna('')
        for index, row in df.iterrows():
            insert_query = f"INSERT INTO {table_name} (address, price, area, bedroom, bathroom, information, id, image) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(insert_query, (
            row['address'], row['price'], row['area'], row['bedroom'], row['bathroom'],
            row['information'], row['id'], row['image']))

        con.commit()
        print("Data imported successfully!")
except Error as e:
    print(f"Error: {e}")
finally:
    # Close the database connection and cursor
    if cursor:
        cursor.close()
    if con and con.is_connected():
        con.close()
        print("MySQL connection is closed.")
