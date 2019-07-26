"""

Zerodha

Python test task:-----------------

BSE publishes a "Bhavcopy" file every day here: https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx

Write a Python script that:- Downloads the Equity bhavcopy zip from the above page-
    Extracts and parses the CSV file in it- Writes the records into Redis into appropriate data structures
    (Fields: code, name, open, high, low, close)

Write a simple CherryPy python web application that:- Renders an HTML5 + CSS3 page that lists the top 10 stock entries
 from the Redis DB in a table- Has a searchbox that lets you search the entries by the 'name' field in Redis and
 renders it in a table- Make the page look nice!

Commit the code to Github. Host the application on AWS or Heroku or a similar provider and share both the links.

"""
import os
import requests
import csv

from bs4 import BeautifulSoup
from io import BytesIO
from zipfile import ZipFile

from myredis import MyRedis


class Crawler(object):
    # https://www.bseindia.com/, ContentPlaceHolder1_btnhylZip
    def __init__(self, url, download_link_id, field_csv_map, redis_instance):

        self.url = url
        self.download_link_id = download_link_id
        self.field_csv_map = field_csv_map
        self.myredis = redis_instance

    def get_download_page(self, page_path):  # /markets/MarketInfo/BhavCopy.aspx
        response = requests.get(self.url+page_path)
        page = response.content
        return page

    def get_download_url(self, page):  # /download/BhavCopy/Equity/EQ250719_CSV.ZIP
        parsed_page = BeautifulSoup(page, 'html.parser')
        download_path = parsed_page.find(id=self.download_link_id).get('href')
        return download_path

    @staticmethod
    def get_zip(file_url):
        try:
            res = requests.get(file_url)
            zipped_file = ZipFile(BytesIO(res.content))
            return zipped_file
        except:  # TODO : Handle individual exceptions, create a decorator
            raise Exception("Some error in downloading processing zip file")

    @staticmethod
    def extract_csv_from_zip(zipped_file):
        csv_filename = zipped_file.infolist()[0].filename
        op_file_name = zipped_file.extract(csv_filename)
        zipped_file.close()
        return op_file_name

    def process_csv_to_db(self, csv_file):
        with open(csv_file) as stocks:
            csv_reader = csv.reader(stocks, delimiter=",")
            field_index = dict()
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    for field in self.field_csv_map.keys():
                        field_index[field] = row.index(self.field_csv_map[field])
                else:
                    data = dict()
                    for field, index in field_index.items():
                        field_value = row[index]
                        if isinstance(field_value, str):
                            field_value = field_value.strip()
                        data[field] = field_value
                    self.myredis.create_stock(data)
                line_count += 1

    def delete(self, csv_file):
        os.remove(csv_file)


if __name__ == "__main__":
    BASE_URL = "https://www.bseindia.com/"
    HOMEPAGE_PATH = "/markets/MarketInfo/BhavCopy.aspx"
    DOWNLOAD_LINK_ELEMENT_ID = "ContentPlaceHolder1_btnhylZip"
    FIELD_CSV_MAP = {
        'code': 'SC_CODE',
        'name': 'SC_NAME',
        'open': 'OPEN',
        'high': 'HIGH',
        'low': 'LOW',
        'close': 'CLOSE'
    }  # add if needed 'turnover': 'NET_TURNOV'
    r = MyRedis()
    crawler = Crawler(BASE_URL, DOWNLOAD_LINK_ELEMENT_ID, FIELD_CSV_MAP, r)
    home_page = crawler.get_download_page(HOMEPAGE_PATH)
    zip_dload_url = crawler.get_download_url(home_page)
    zipfile_obj = crawler.get_zip(zip_dload_url)
    extracted_csv_filename = crawler.extract_csv_from_zip(zipfile_obj)
    crawler.process_csv_to_db(extracted_csv_filename)
    crawler.delete(extracted_csv_filename)
