# Load connect to database

import pymysql
from elasticsearch import Elasticsearch
import time

HOST = '192.168.1.65'

class Config:

    def __init__(self):
        self.reload()

    def reload(self):
        # MySQL
        self.mysql_conn = pymysql.connect(
            host=HOST, user='root', password='vnistadmin', db='webix', charset='utf8mb4')
        self.mysql_cur = self.mysql_conn.cursor()

        # ElasticSearch
        self.es = Elasticsearch(['%s:9200' % HOST])


if __name__ == '__main__':
    c = Config()
