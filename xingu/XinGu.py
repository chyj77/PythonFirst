import requests
import random
import time
import socket
import http.client
import pymysql
import lxml.etree

class XinGu:

    def get_content(url, data=None):
        header = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': '__utma=156575163.2123015184.1517402961.1517402961.1517402961.1; __utmc=156575163; __utmz=156575163.1517402961.1.1.utmcsr=10jqka.com.cn|utmccn=(referral)|utmcmd=referral|utmcct=/; Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1517466776; spversion=20130314; searchGuide=sg; cmsad_170_0=0; cmsad_171_0=0; cmsad_172_0=0; Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1=1517560899; Hm_lvt_22a3c65fd214b0d5fd3a923be29458c7=1517466872,1517467117,1517467158,1517560899; Hm_lpvt_22a3c65fd214b0d5fd3a923be29458c7=1517560899; historystock=603516%7C*%7C002587%7C*%7C002610%7C*%7C603506; v=Ak9rwpAsSTIKik1Oeuh1hLpE3uhddKPRPcinimFc677FMGXeaUQz5k2YN_tz',
            'DNT': '1',
            'Host': 'data.10jqka.com.cn',
            'If-Modified-Since': 'Thu, 29 Mar 2018 08:58:08 GMT',
            'If-None-Match': 'W/"5abcaaa0-46ef7"',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3315.4 Safari/537.36',
        }
        timeout = random.choice(range(80, 180))
        while True:
            try:
                rep = requests.get(url, headers=header, timeout=timeout)
                rep.encoding = 'GBK'
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

    def get_data(html_text):
        tree = lxml.etree.HTML(html_text)
        tbody = tree.xpath('//a[@class="blue"]')
        index = 0
        stocks ={}
        for td in tbody:
            if(td.text != '公告'):
                # print(td.text)
                if(index%2 == 0):
                    stocks["code"] = td.text
                else:
                    stocks["name"]=td.text
                    db = pymysql.connect(host='123.206.87.88', port=3306, user='root', password='keke2012', db='mystockdb',charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
                    # 使用cursor()方法获取操作游标
                    cursor = db.cursor()
                    sql = "select stockcode from  dict_stock where stockcode = '"+ stocks["code"]+"'"
                    # print(sql)
                    #查询数据库多条数据
                    cursor.execute(sql)
                    result = cursor.fetchone()
                    if (result==None) :
                        print(stocks)
                        insertSql = "insert into dict_stock (stockcode,stockname) VALUES ('"+stocks['code']+"','"+stocks['name']+"')"
                        print(insertSql)
                        cursor.execute(insertSql)
                        db.commit()
                index = index+1


    def play(self):
        # db = pymysql.connect(host='123.206.87.88', port=3306, user='root', password='keke2012', db='mystockdb',charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        # # 使用cursor()方法获取操作游标
        # cursor = db.cursor()
        # sql = 'select stockcode from  market_stock'
        # #查询数据库多条数据
        # cursor.execute(sql)
        # result = cursor.fetchall()
        # for data in result:
        url = 'http://data.10jqka.com.cn/ipo/xgsgyzq/'
        # print(url)
        self.get_data(self.get_content(url))


    # 关闭数据连接
    # db.close()

