import requests
import random
import time
import socket
import http.client
import pymysql
import lxml.etree
import json


class xinguInfo:

    def queryDB(self):
        db = pymysql.connect(host='123.206.87.88', port=3306, user='root', password='keke2012', db='mystockdb',
                             charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        # 使用cursor()方法获取操作游标
        cursor = db.cursor()
        sql = "select stockcode from  dict_stock where marketTime is null "
        # print(sql)
        # 查询数据库多条数据
        cursor.execute(sql)
        result = cursor.fetchall()
        try:
            for data in result:
                stockcode = data["stockcode"]
                if (stockcode.startswith('60', 0, 2)):
                    shHtml = self.shInfo(self, stockcode)
                    jsonCallBack = shHtml.replace('jsonpCallback36992(', '')
                    jsonCallBack = jsonCallBack[:-1]
                    python_to_json = json.loads(jsonCallBack)
                    pageHelp = python_to_json['pageHelp']
                    if (pageHelp != None):
                        pageData = pageHelp['data']
                        if (pageData != None and len(pageData) > 0):
                            # print(pageData)
                            totalFlowShares = pageData[0]["totalFlowShares"].replace(",","")
                            LISTINGDATE = pageData[0]["LISTING_DATE"]
                            COMPANYCODE = pageData[0]["COMPANY_CODE"]
                            totalShares = pageData[0]["totalShares"].replace(",","")
                            print(LISTINGDATE, totalShares, totalFlowShares, COMPANYCODE)
                            updateSql = "update dict_stock set marketTime='%s', totalNum=%s,floatNum=%s where stockcode='%s'" % (
                            LISTINGDATE, totalShares, totalFlowShares, stockcode)
                            print(updateSql)
                            cursor.execute(updateSql)
                            db.commit()
                else:
                    szHtml = self.szInfo(self, stockcode)
                    tree = lxml.etree.HTML(szHtml)
                    tbody = tree.xpath('//table[@id="REPORTID_tab2"]/tr/td')
                    if (tbody[0].text != '没有找到符合条件的数据！'):
                        COMPANYCODE = tbody[2].text
                        LISTINGDATE = tbody[4].text
                        totalShares = tbody[5].text.replace(",","")
                        totalFlowShares = tbody[6].text.replace(",","")
                        hangye = tbody[7].text
                        print(COMPANYCODE, LISTINGDATE, totalShares, totalFlowShares, hangye)
                        updateSql = "update dict_stock set marketTime='%s', totalNum=%s,floatNum=%s,hangye='%s' " \
                                    "where stockcode='%s'" % (LISTINGDATE, totalShares, totalFlowShares, hangye, stockcode)
                        print(updateSql)
                        cursor.execute(updateSql)
                        db.commit()
        except Exception:
            # 发生错误时回滚
            print(Exception)
            db.rollback()

        # 关闭数据库连接
        db.close()

    def shInfo(self, stockcode):
        t = time.time()
        int(round(t * 1000))
        url = 'http://query.sse.com.cn/security/stock/getStockListData.do?&jsonCallBack=jsonpCallback36992' \
              '&isPagination=true&stockCode=%s' \
              '&csrcCode=&areaName=&stockType=1' \
              '&pageHelp.cacheSize=1&pageHelp.beginPage=1' \
              '&pageHelp.pageSize=25&pageHelp.pageNo=1&_=%s' % (stockcode, t)
        # print(self.getshHtml(url))
        return self.getshHtml(url)


    def szInfo(self, stockcode):
        t = time.time()
        int(round(t * 1000))
        url = 'http://www.szse.cn/szseWeb/FrontController.szse?randnum=0.5669558818962805'
        # print(self.getszHtml(url,stockcode))
        return self.getszHtml(url, stockcode)


    def getshHtml(url):
        header = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': 'yfx_c_g_u_id_10000042=_ck18022809153419407403135993657; yfx_mr_10000042=%3A%3Amarket_type_free_search%3A%3A%3A%3Abaidu%3A%3A%3A%3A%3A%3A%3A%3Awww.baidu.com%3A%3A%3A%3Apmf_from_free_search; yfx_key_10000042=; yfx_mr_f_10000042=%3A%3Amarket_type_free_search%3A%3A%3A%3Abaidu%3A%3A%3A%3A%3A%3A%3A%3Awww.baidu.com%3A%3A%3A%3Apmf_from_free_search; yfx_f_l_v_t_10000042=f_t_1519780534914__r_t_1522634648180__v_t_1522634648180__r_c_3; VISITED_STOCK_CODE=%5B%22600000%22%2C%22601206%22%2C%22600004%22%2C%22600008%22%2C%22601007%22%2C%22603009%22%2C%22600929%22%2C%22600996%22%5D; VISITED_COMPANY_CODE=%5B%22600000%22%2C%22601206%22%2C%22600004%22%2C%22600008%22%2C%22601007%22%2C%22603009%22%2C%22600929%22%2C%22600996%22%5D; seecookie=%5B600000%5D%3A%u6D66%u53D1%u94F6%u884C%2C%5B600004%5D%3A%u767D%u4E91%u673A%u573A%2C%5B600008%5D%3A%u9996%u521B%u80A1%u4EFD%2C%5B601007%5D%3A%u91D1%u9675%u996D%u5E97%2C%5B603009%5D%3A%u5317%u7279%u79D1%u6280%2C%5B600929%5D%3A%u6E56%u5357%u76D0%u4E1A%2C%5B600996%5D%3A%u8D35%u5E7F%u7F51%u7EDC; VISITED_MENU=%5B%228535%22%2C%228443%22%2C%2210884%22%2C%229729%22%2C%2210883%22%2C%228350%22%2C%228530%22%2C%229062%22%2C%228529%22%2C%228528%22%2C%229055%22%5D',
            'DNT': '1',
            'Host': 'query.sse.com.cn',
            'Referer': 'http://www.sse.com.cn/assortment/stock/list/share/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3315.4 Safari/537.36',
        }
        timeout = random.choice(range(80, 180))
        while True:
            try:
                rep = requests.get(url, headers=header, timeout=timeout)
                rep.encoding = 'utf-8'
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


    def getszHtml(url, stockcode):
        header = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'DNT': '1',
            'Host': 'www.szse.cn',
            'Origin': 'http://www.szse.cn',
            'Referer': 'http://www.szse.cn/main/marketdata/jypz/colist/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3315.4 Safari/537.36'
        }
        timeout = random.choice(range(80, 180))
        while True:
            try:
                data = {
                    'TABKEY': 'tab2',
                    'tab1PAGENO': '1',
                    'txtDMorJC': stockcode,
                    'CATALOGID': '1110',
                    'AJAX': 'AJAX-TRUE',
                    'ACTIONID': '7',
                }
                rep = requests.post(url, headers=header, data=data, timeout=timeout)
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



