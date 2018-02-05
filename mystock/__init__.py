import requests
import random
import time
import socket
import http.client
import pymysql
from bs4 import BeautifulSoup


def get_pages(url, data=None):
    header = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Cookie': 'log=; __utma=156575163.2123015184.1517402961.1517402961.1517402961.1; __utmc=156575163; __utmz=156575163.1517402961.1.1.utmcsr=10jqka.com.cn|utmccn=(referral)|utmcmd=referral|utmcct=/; v=AvreQZWTdATgsPho8P14F4dDSysYq33R8C_yDATzpg1Y95Dd7DvOlcC_QvHW',
        'DNT': '1',
        'Host': 'q.10jqka.com.cn',
        'If-Modified-Since': 'Thu, 11 Jan 2018 07:02:01 GMT',
        'If-None-Match': 'W/"5a570be9-95e6"',
        'Referer': 'http://stock.10jqka.com.cn/',
        'Upgrade-Insecure-Requests': '1',
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
    bs = BeautifulSoup(rep.text, "html.parser")
    contents = bs.contents[7].contents[12].attrs['page']
    return contents

def get_content(url, data=None):
    header = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Cookie': 'log=; __utma=156575163.2123015184.1517402961.1517402961.1517402961.1; __utmc=156575163; __utmz=156575163.1517402961.1.1.utmcsr=10jqka.com.cn|utmccn=(referral)|utmcmd=referral|utmcct=/; v=AvreQZWTdATgsPho8P14F4dDSysYq33R8C_yDATzpg1Y95Dd7DvOlcC_QvHW',
        'DNT': '1',
        'Host': 'q.10jqka.com.cn',
        'If-Modified-Since': 'Thu, 11 Jan 2018 07:02:01 GMT',
        'If-None-Match': 'W/"5a570be9-95e6"',
        'Referer': 'http://stock.10jqka.com.cn/',
        'Upgrade-Insecure-Requests': '1',
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
    # return html_text
def get_data(html_text):
    # final = []
    bs = BeautifulSoup(html_text, "html.parser")  # 创建BeautifulSoup对象
    contents = bs.contents[1].contents[3].contents # 获取body部分
    # page = data.find('page')  # 获取ul部分
    # li = ul.find_all('li')  # 获取所有的li
    dict ={}
    for content in contents: # 对每个li标签中的内容进行遍历

        if content.string != '\n':
            code = content.contents[3].contents[0].contents[0] # 找到股票代码
            name = content.contents[5].contents[0].contents[0] # 找到股票名称
            dict[code] = name  # 添加到temp中
            # final.append(dict)   #将temp加到final中
    saveToDB(dict)
    return dict

def saveToDB(dict):
    # 打开数据库连接
    db = pymysql.connect(host='192.168.203.128', port=3306, user='root', password='keke2012', db='mystockdb',charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    sql = 'INSERT INTO market_stock(stockcode, stockname) VALUES '
    values = ''
    for key, value in dict.items():
        # SQL 插入语句
        values = values + '("%s", "%s"),' %(key, value)
    try:
        values = values.rstrip(",")
        execSql = sql + values
        # 执行sql语句
        cursor.execute(execSql)
        # 执行sql语句
        db.commit()
    except Exception:
        # 发生错误时回滚
        print(Exception)
        db.rollback()

# 关闭数据库连接
    db.close()