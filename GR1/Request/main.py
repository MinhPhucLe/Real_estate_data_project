import mysql.connector
from app import app
from config import mysql
from flask import jsonify
from flask import flash, request
from datetime import datetime as dt
from mysql.connector import Error

con = None
cursor = None
real_estate_data = []

try:
    con = mysql.connect()
    print("Database connected!")
    cursor = con.cursor()
    select_query = "SELECT * FROM real_estate"
    cursor.execute(select_query)
    # Fetch all the IDs
    all_ids = cursor.fetchall()
    for id in all_ids:
      real_estate_data.append((id[0], id[1], id[2], id[6]))
    con.commit()
    #print("Data imported successfully!")
except Error as e:
    print(f"Error: {e}")
finally:
    # Close the database connection and cursor
    if cursor:
        cursor.close()
    if con:
        con.close()
        print("MySQL connection is closed.")

@app.route('/create', methods=['POST'])
def create_req():
    conn = None
    cursor = None
    try:
        json = request.json
        address = json['address']
        min_area = json['min_area']
        max_area = json['max_area']
        min_price = json['min_price']
        max_price = json['max_price']
        mail = json['email']
        accpt = json['accpt']
        if address and mail and request.method == 'POST':
            conn = mysql.connect()
            cursor = conn.cursor()
            now = dt.now()
            formatted_now = now.strftime('%Y-%m-%d %H:%M:%S')
            print("Formatted Date and Time:", formatted_now)
            print(formatted_now)
            sqlQuery = "INSERT INTO user_request (address, min_area, max_area, min_price, max_price, mail, accpt, date_time) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
            bindData = (address, min_area, max_area, min_price, max_price, mail, accpt, formatted_now)
            cursor.execute(sqlQuery, bindData)
            conn.commit()
            respone = jsonify('Request of user added successfully!')
            respone.status_code = 200
            return respone
        else:
            print("Conditions not met")
            return showMessage()
    except Exception as e:
        print(e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/req')
def user_details():
    cursor = None
    conn = None
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM user_request")
        userRow = cursor.fetchall()
        respone = jsonify(userRow)
        respone.status_code = 200
        return respone
    except Exception as e:
        print(e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/req/address/<addr>')
def user_req_details(addr):
    cursor = None
    conn = None
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM real_estate WHERE address like %s", ('%' + addr + '%',))
        reqRow = cursor.fetchall()
        respone = jsonify(reqRow)
        respone.status_code = 200
        return respone
    except Exception as e:
        print(e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/req/details', methods = ['GET'])
def usr_detail_request():
    json = request.json
    address = json['address']
    min_area = json['min_area']
    max_area = json['max_area']
    min_price = json['min_price']
    max_price = json['max_price']
    accept = json['accpt']
    filtered_data = [
        proper_data for proper_data in real_estate_data
        if address in proper_data[0]
        if min_area <= proper_data[2] <= max_area
        if min_price <= proper_data[1] <= max_price
        if accept == 0
    ]

    another_list = [
        proper_data for proper_data in real_estate_data
        if address in proper_data[0]
        if accept == 1
        if proper_data[1] == 0 or proper_data[2] == 0
    ]

    filtered_data += another_list

    return jsonify(filtered_data)


@app.route('/req/email/<mail>')
def user_req_details_mail(mail):
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        print(mail)
        cursor.execute("SELECT * from user_request where mail = %s", mail)
        reqRow = cursor.fetchall()
        respone = jsonify(reqRow)
        respone.status_code = 200
        return respone
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()
@app.errorhandler(404)
def showMessage(error=None):
    message = {
        'status': 404,
        'message': 'Record not found: ' + request.url,
    }
    respone = jsonify(message)
    respone.status_code = 404
    return respone

if __name__ == "__main__":
    app.run()