# coding=gbk
from apscheduler.schedulers.blocking import BlockingScheduler
from bs4 import BeautifulSoup
import time
import socket
import random
import requests
import http.client
import re
import os
import aliai
import pymongo
import redis

def getHtml(url):
    header = {
        'Accept': 'text/html, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Cookie': '__utmc=156575163; spversion=20130314; searchGuide=sg; __utma=156575163.2123015184.1517402961.1518405764.1518418592.3; __utmz=156575163.1518418592.3.3.utmcsr=10jqka.com.cn|utmccn=(referral)|utmcmd=referral|utmcct=/; historystock=1A0001%7C*%7C601519%7C*%7C000932%7C*%7C603516%7C*%7C002587; refreshStat=off; Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1519724465,1519813694,1519874903,1519874926; Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1=1519874926; Hm_lvt_60bad21af9c824a4a0530d5dbf4357ca=1519724465,1519813694,1519874903,1519874926; Hm_lpvt_60bad21af9c824a4a0530d5dbf4357ca=1519874926; Hm_lvt_f79b64788a4e377c608617fba4c736e2=1519724465,1519813694,1519874903,1519874926; Hm_lpvt_f79b64788a4e377c608617fba4c736e2=1519874926; v=AmVBANbaU_Ok8rcvr3k_phQKdCqbohrCY1P9pGdKIRyrfo8cL_IpBPOmDV30',
        'DNT': '1',
        'hexin-v': 'AmVBANbaU_Ok8rcvr3k_phQKdCqbohrCY1P9pGdKIRyrfo8cL_IpBPOmDV30',
        'Host': 'stock.10jqka.com.cn',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3315.4 Safari/537.36',
    }
    timeout = random.choice(range(80, 180))
    while True:
        try:
            rep = requests.get(url, headers=header, timeout=timeout)
            rep.encoding = 'gbk'
            # req = urllib.request.Request(url, data, header)
            # response = urllib.request.urlopen(req, timeout=timeout)
            # html1 = response.read().decode('UTF-8', errors='ignore')
            # response.close()
            break
        # except urllib.request.HTTPError as e:
        #         print( '1:', e)
        #         time.sleep(random.choice(range(5, 10)))
        #
        # except urllib.request.URLError as e:
        #     print( '2:', e)
        #     time.sleep(random.choice(range(5, 10)))
        except socket.timeout as e:
            print('3:', e)
            time.sleep(random.choice(range(8, 15)))

        except socket.error as e:
            print('4:', e)
            time.sleep(random.choice(range(20, 60)))

        except http.client.BadStatusLine as e:
            print('5:', e)
            time.sleep(random.choice(range(30, 80)))

        except http.client.IncompleteRead as e:
            print('6:', e)
            time.sleep(random.choice(range(5, 15)))
    # print('测试:',rep.text)
    return rep.text

def getFupan(htmlText):
    bs = BeautifulSoup(htmlText, "html.parser")
    fupanUrl = bs.find_all('a', title=re.compile(r'^涨停复盘*'))
    fupanRq = bs.find('a', title=re.compile(r'^涨停复盘*')).next_sibling.next_sibling
    rq = fupanRq.text.split(" ")[0]
    print("日期：", rq)
    today = time.strftime('%m-%d', time.localtime(time.time()))
    print(today)
    if(today == rq):
        for obj in fupanUrl:
            href = obj.attrs['href']
            print(href)
            return href

def getPngUrl(htmlText):
    bs = BeautifulSoup(htmlText, "html.parser")
    img = bs.find_all('img', src=re.compile(r'^http://u.thsi.cn/fileupload/data/Input/2018/.*?png$'))
    for obj in img:
        pngUrl = obj.attrs["src"]
        # print(pngUrl)
        return pngUrl

def downLoad(pngUrl):
    if not os.path.exists('image'):
        os.makedirs('image')
    try:
        print(pngUrl)
        pic = requests.get(pngUrl,timeout=5)  #超时异常判断 5秒超时
    except requests.exceptions.ConnectionError:
        print('当前图片无法下载')

    file_name = "image/fupan.png" #拼接图片名
    print(file_name)
    #将图片存入本地
    fp = open(file_name,'wb')
    fp.write(pic.content) #写入图片
    print(fp.name)
    filePath = fp.name
    fp.close()
    return filePath

def store(fupanText):
    fupanText = fupanText.replace("\n","")
    bs = BeautifulSoup(fupanText, "html.parser")
    table = bs.find('table', id="table_0")
    array = []
    for tr in table:
        jsonStr = {}
        try:
            td1 = tr.contents[0].text
            td2 = tr.contents[1].text
            if(td1 != None and td1 !=''and td2 != None and td2 !=''):
                pattern = re.compile(r'\d+')
                result1=pattern.findall(td1)
                # print(result1)
                jsonStr["stockcode"] = result1[0]
                jsonStr["stockname"] = td2
                jsonStr["nowprice"] = tr.contents[2].text
                jsonStr["ztsj"] = tr.contents[3].text
                jsonStr["ztmx"] = ''  # tr.contents[4].text
                jsonStr["ztts"] = tr.contents[4].text
                jsonStr["ztgn"] = tr.contents[5].text
                # print(jsonStr)
                array.append(jsonStr)
        except Exception as e:
            print(e)
    print(array)
    today = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    # today ="2018-04-25"
    mongodata = {"rq": today, "data": array}
    # saveMongoDB(mongodata)
    saveRedis(mongodata)

def saveMongoDB(mongodata):
    # client = pymongo.MongoClient(host='172.21.0.17', port=27017)
    client = pymongo.MongoClient(host='123.206.87.88', port=27017)
    db = client.mystock
    collection = db.fupan
    collection.insert_one(mongodata)

def saveRedis(mongodata):
    pool = redis.ConnectionPool(host='123.206.87.88', port=6379, password='keke2012', db=0)
    r = redis.Redis(connection_pool=pool)
    key = "fupan:" + mongodata['rq']
    r.setnx(key,mongodata)

if __name__ == '__main__':
    url = "http://stock.10jqka.com.cn/fupan/"
    fupanUrl = getFupan(getHtml(url))
    if(fupanUrl != None):
        pngUrl = getPngUrl(getHtml(fupanUrl))
        filePath = downLoad(pngUrl)
        fupanText = aliai.demo(filePath)
        store(fupanText)