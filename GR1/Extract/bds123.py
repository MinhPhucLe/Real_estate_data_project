import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv
import pandas as pd
from mysql.connector import Error
import mysql.connector

data_list = []
start_urls = ['https://bds123.vn/ban-nha-ha-noi.html']
baseurl = 'https://bds123.vn/ban-nha-ha-noi.html?page='
base_link = '#main > div.leftCol.mt-3 > section > div.post-listing > li:nth-child('
lastest_house = "/home/leminhphuc/real_estate_data/bds123_latest.txt"
content = ""
temp_latest = ""
check = 1
con = None
cursor = None

check_list = []

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


        select_query = "SELECT id FROM real_estate"
        cursor.execute(select_query)

        # Fetch all the IDs
        all_ids = cursor.fetchall()
        print(len(all_ids))

        con.commit()
        print("Data selected successfully!")
except Error as e:
    print(f"Error: {e}")
finally:
    # Close the database connection and cursor
    if cursor:
        cursor.close()
    if con and con.is_connected():
        con.close()
        print("MySQL connection is closed.")


try:
    with open(lastest_house, 'r') as file:
        content = file.read()
except FileNotFoundError:
        content = ""
for i in range(2, 25):
    url = baseurl + str(i)
    start_urls.append(url)
for i in range(len(start_urls)):
    url = start_urls[i]
    big_data = requests.get(url, timeout=100)
    html = BeautifulSoup(big_data.text, 'html.parser')
    for j in range(1, 21):
        check = 1
        link = base_link + str(j) + ') > a'
        image_link = base_link + str(j) + ') > a > figure > img'
        #print(image_link)
        image = html.select_one(image_link).get('data-src')
        absolute_image_source = urljoin(url, image)
        #print(image_link)
        #print(image)
        #print(absolute_image_source)
        selected_link = html.select_one(link).get('href')
        absolute_link = urljoin(url, selected_link)
        #print(absolute_link)
        access_link = requests.get(absolute_link, timeout=100)
        soup = BeautifulSoup(access_link.text, 'html.parser')
        start = 0
        for index, char in enumerate((absolute_link)):
            if char == '-':
                start = index
        up_to_date_link = absolute_link[start + 1: len(absolute_link) - 5]
        if i == 0 and j == 1:
            temp_latest = up_to_date_link
        try:
            price = soup.select_one('#main > div.the-post.clearfix > div.clearfix.margin-bottom-15 > div.post-features.float-left.clearfix > span.item.post-price').text
        except Exception as e:
            price = ''
        try:
            area = soup.select_one('#main > div.the-post.clearfix > div.clearfix.margin-bottom-15 > div.post-features.float-left.clearfix > span.item.post-acreage').text
        except Exception as e:
            area = ''
        area_m_position = area.find('m')
        if area_m_position == -1:
            continue
        area_dot_position = -1
        for k in range(0, area_m_position):
            if area[k] == '.':
                area_dot_position = k
        #print(area_dot_position, area_m_position)
        area_float = area[0: area_m_position - 1]
        area_float = float(area_float)
        #print(area)
        #print(area_float)
        if area_dot_position >= 0:
            num_zero = area_m_position - (area_dot_position + 1) - 1
            #print('here')
            #print(num_zero)
            if num_zero >= 3:
                for k in range(num_zero):
                    area_float = area_float * 10
        area_float = round(area_float)
        #print(area_float)
        try:
            bedroom = soup.select_one('#main > div.the-post.clearfix > div.clearfix.margin-bottom-15 > div.post-features.float-left.clearfix > span.item.post-bedroom').text
        except Exception as e:
            bedroom = ''
        try:
            bathroom = soup.select_one('#main > div.the-post.clearfix > div.clearfix.margin-bottom-15 > div.post-features.float-left.clearfix > span.item.post-bathroom').text
        except Exception as e:
            bathroom = ''
        try:
            address = soup.select_one('#main > div.the-post.clearfix > p > span:nth-child(2)').text
        except Exception as e:
            address = ''
        adress_length = len(address)
        address = address[9: adress_length + 1]
        if ('tỷ' not in price and 'triệu' not in price and 'Thỏa thuận' not in price):
            continue
        #print(i, ' ', j)
        if ('Thỏa thuận' in price):
            num_price = 0
        else:
            pos_space = price.find('t')
            num_price = price[0: pos_space -1]
            if (len(price) - pos_space > 2):
                num_price = float(num_price) * 1000000
            else:
                num_price = float(num_price) * 1000000000
            num_price = int(num_price)
        #print(price)
        #print(num_price)
        #try:
            #description = soup.select_one('#main > div.the-post.clearfix > div.leftCol > div:nth-child(3) > div').text
        #except Exception as e:
            #description = ''
        #print(description)
        my_data = {"address": address, "price": num_price, "area": area_float, "bedroom": bedroom, "bathroom": bathroom, "information": absolute_link, "id": up_to_date_link, "image": image}
        for id in all_ids:
            if up_to_date_link == id[0]:
                print(up_to_date_link)
                check = 0
                break
        for id in check_list:
            if up_to_date_link == check_list:
                check = 0
                break
        if check == 1:
            check_list.append(up_to_date_link)
            data_list.append(my_data)
    csv_file_path = "/home/leminhphuc/real_estate_data/bds123.csv"
    if data_list:
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            # Create a CSV writer
            csv_writer = csv.DictWriter(csv_file, fieldnames=data_list[0].keys())

            # Write header
            csv_writer.writeheader()

            # Write data rows
            csv_writer.writerows(data_list)

    with open(lastest_house, 'w') as file:
        file.write(temp_latest)
