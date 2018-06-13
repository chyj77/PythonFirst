# coding=gbk
from bs4 import BeautifulSoup
import time
import threading
import socket
import random
import requests
import http.client
import re
import os
import aliai
import pymongo


def store(fupanText):
    fupanText = fupanText.replace("\n", "")
    bs = BeautifulSoup(fupanText, "html.parser")
    table = bs.find('table', id="table_0")
    array = []
    for tr in table:
        jsonStr = {}
        td1 = tr.contents[0].text
        td2 = tr.contents[1].text
        if (td1 != None and td1 != '' and td2 != None and td2 != ''):
            jsonStr["stockname"] = td1
            jsonStr["top"] = td2
            jsonStr["quarter1"] = tr.contents[2].text
            jsonStr["beyond"] = tr.contents[3].text
            jsonStr["dtsyl"] = tr.contents[4].text
            # print(jsonStr)
            array.append(jsonStr)
    print(array)
    today = time.strftime('%Y-%m', time.localtime(time.time()))
    # today ="2018-04-25"
    mongodata = {"rq": today, "data": array}
    saveMongoDB(mongodata)


def saveMongoDB(mongodata):
    # client = pymongo.MongoClient(host='172.21.0.17', port=27017)
    client = pymongo.MongoClient(host='*************', port=27017)
    db = client.mystock
    collection = db.jrj
    collection.insert_one(mongodata)


if __name__ == '__main__':
    filePath = "image/201804261123001879222188.jpg"
    fupanText = aliai.demo(filePath)
    store(fupanText)
