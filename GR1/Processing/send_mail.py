import csv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import mysql.connector
from mysql.connector import Error

def send_email(subject, body, to_email):
    # Your email credentials
    sender_email = "tryrequestamin123@gmail.com"
    sender_password = "xfqs tjvl obak wovv"

    # Create the MIME object
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = to_email
    msg['Subject'] = subject

    # Attach the body of the email
    msg.attach(MIMEText(body, 'plain'))

    # Connect to the SMTP server
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(sender_email, sender_password)  # Login to your email account
        server.sendmail(sender_email, to_email, msg.as_string())  # Send the email

another_lists = []

def read_file(filepath, addr, min_p, max_p, min_a, max_a):
    list_accepted = []
    file = open(filepath, 'r', encoding='utf-8')
    csv_reader = csv.reader(file)
    for row in csv_reader:
        if row[1] != '0' and row[2] != '0':
            if addr in row[0] and min_p <= int(row[1]) and max_p >= int(row[1]) and min_a <= float(row[2]) and max_a >= float(row[2]):
                selected_field = (row[0], row[1], row[2], row[5])
                list_accepted.append(" - ".join(selected_field))  # Join the inner list into a string
        else:
            if addr in row[0]:
                another_list = (row[0], row[1], row[2], row[5])
                another_lists.append(" - ".join(another_list))
    file.close()
    return list_accepted

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
        request_query = "select * from user_request"
        cursor.execute(request_query)
        results = cursor.fetchall()
        pos = 0
        for row in results:
            pos = pos + 1
            print(pos)
            addr = row[0]
            min_a = float(row[1])
            max_a = float(row[2])
            min_p = int(row[3])
            max_p = int(row[4])
            another_lists = []
            list_accepted = read_file('/home/leminhphuc/real_estate_data/real_estate.csv', addr, min_p, max_p, min_a, max_a)
            if (int(row[6]) == 1):
                #print(another_lists)
                list_accepted += another_lists
            if list_accepted == []:
                continue
            to_mail = row[5]
            email_body = "Address - Price - Area - Additional Info \n"
            email_body += "\n".join(list_accepted)
            send_email("Real estate meeting your request.", email_body, to_mail)
            print("--------------------------------------------------------------")
        con.commit()
        print("Data sent successfully!")
except Error as e:
    print(f"Error: {e}")
finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if con and con.is_connected():
        con.close()
        print("MySQL connection is closed.")
