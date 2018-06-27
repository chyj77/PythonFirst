import requests
import random
import time
import socket
import http.client
import pymysql
import lxml.etree


class Thsgn:
    db = pymysql.connect(host='***.***.***.***', port=3306, user='root', password='********', db='mystockdb',charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    def get_content(url, data=None):
        header = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': '__utmc=156575163; searchGuide=sg; __utma=156575163.2123015184.1517402961.1518405764.1518418592.3; __utmz=156575163.1518418592.3.3.utmcsr=10jqka.com.cn|utmccn=(referral)|utmcmd=referral|utmcct=/; Hm_lvt_f79b64788a4e377c608617fba4c736e2=1519972643; Hm_lpvt_f79b64788a4e377c608617fba4c736e2=1519972643; log=; Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1522317424; cmsad_170_0=0; cmsad_171_0=0; cmsad_172_0=0; Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1=1523602190; Hm_lvt_22a3c65fd214b0d5fd3a923be29458c7=1523602190; Hm_lpvt_22a3c65fd214b0d5fd3a923be29458c7=1523602190; historystock=002235; spversion=20130314; v=Ap25-N7im-wbUn_AmwDnzpzirHKTutGs2-815F9i3HciOrekJwrh3Gs-RY_s',
            'DNT': '1',
            'Host': 'stockpage.10jqka.com.cn',
            'Referer': 'http://q.10jqka.com.cn/gn/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3315.4 Safari/537.36',
        }
        timeout = random.choice(range(80, 180))
        while True:
            try:
                rep = requests.get(url, headers=header, timeout=timeout)
                rep.encoding = 'UTF-8'
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

    def get_data(self,html_text,stockcode):
        tree = lxml.etree.HTML(html_text)
        tbody = tree.xpath('//dl[@class="company_details"]/*')
        try:
            dd = tbody[3]
            attrib = dd.attrib
            title = attrib['title']
            cursor = self.db.cursor()
            sql = "update dict_stock set tag = '%s' where stockcode='%s'"%(title,stockcode)
            print(sql)
            cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print(e)
            self.db.rollback()

    def play(self) -> object:
         # 使用cursor()方法获取操作游标
         cursor = self.db.cursor()
         sql = 'select stockcode from  dict_stock'
         #查询数据库多条数据
         cursor.execute(sql)
         result = cursor.fetchall()
         for data in result:
            url = 'http://stockpage.10jqka.com.cn/%s/'%(data['stockcode'])
            print(url)
            self.get_data(self,self.get_content(url),data['stockcode'])

    # 关闭数据连接
    # db.close()
