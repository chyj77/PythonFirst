from bs4 import BeautifulSoup
import requests
import random
import time
import socket
import http.client
import pymongo

class CrawlerTHS():
    def getHtml(url):
        header = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': '__utma=156575163.2123015184.1517402961.1517402961.1517402961.1; __utmc=156575163; __utmz=156575163.1517402961.1.1.utmcsr=10jqka.com.cn|utmccn=(referral)|utmcmd=referral|utmcct=/; Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1517466776; spversion=20130314; searchGuide=sg; cmsad_170_0=0; cmsad_171_0=0; cmsad_172_0=0; Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1=1517560899; Hm_lvt_22a3c65fd214b0d5fd3a923be29458c7=1517466872,1517467117,1517467158,1517560899; Hm_lpvt_22a3c65fd214b0d5fd3a923be29458c7=1517560899; historystock=603516%7C*%7C002587%7C*%7C002610%7C*%7C603506; v=Ak9rwpAsSTIKik1Oeuh1hLpE3uhddKPRPcinimFc677FMGXeaUQz5k2YN_tz',
            'DNT': '1',
            'Host': 'stock.10jqka.com.cn',
            'If-Modified-Since': 'Thu, 11 Jan 2018 07:02:01 GMT',
            'If-None-Match': 'W/"5a7418fc-104c9"',
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

    def getLhbHtml(url):
        header = {
            # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            # 'Accept-Encoding': 'gzip, deflate',
            # 'Accept-Language': 'zh-CN,zh;q=0.9',
            # 'Cache-Control': 'max-age=0',
            # 'Connection': 'keep-alive',
            # 'Cookie': '__utmc=156575163; spversion=20130314; searchGuide=sg; Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1517466776,1517819559,1517819570; Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1=1517819570; historystock=601519%7C*%7C000932%7C*%7C603516%7C*%7C002587; __utma=156575163.2123015184.1517402961.1517402961.1518405764.2; __utmz=156575163.1518405764.2.2.utmcsr=10jqka.com.cn|utmccn=(referral)|utmcmd=referral|utmcct=/; refreshStat=off; v=AvPX9tTQrRsDqWFPp4yRqDbwgvwZKIfqQbzLHqWQT5JJpBmqLfgXOlGMW2-3',
            # 'DNT': '1',
            # 'Host': 'stock.10jqka.com.cn',
            # 'If-Modified-Since': 'Fri, 09 Feb 2018 10:39:05 GMT',
            # 'If-None-Match': 'W/"5a7d7a49-bf500"',
            # 'Upgrade-Insecure-Requests': '1',
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

    def get_data(html_text):
        # final = []
        bs = BeautifulSoup(html_text, "html.parser")
        longHuShRtime = bs.find('input',id='longHuShRtime')
        longHuSzRtime = bs.find('input',id='longHuSzRtime')
        print(longHuShRtime.attrs['value'])
        print(longHuSzRtime.attrs['value'])
        body = bs.find("div", id="lhb")
        sh = body.find('div', attrs={'class': 'block-table fl'})
        sz = body.find('div', attrs={'class': 'block-table fr'})
        shlhbTable = sh.find('table', attrs={'class': 'm-table'})
        shlhb = shlhbTable.find_all('tr')
        for tr in shlhb :
            td = tr.find_all("td")
            for content in td :
                print(content.string)
                contents = content.contents[0]
                if( not isinstance(contents ,str)) :
                    lhbUrl = content.contents[0].attrs["href"]
                    lhbUrl = lhbUrl.replace('cjmx','code')
                # print(lhbUrl)
                # lhbUrl='http://data.10jqka.com.cn/market/longhu/code/603871/'
                    CrawlerTHS.get_lhbData(CrawlerTHS.getLhbHtml(lhbUrl))
        szlhbTable = sz.find('table', attrs={'class': 'm-table'})
        szlhb = szlhbTable.find_all('tr')
        for tr in szlhb :
            td = tr.find_all("td")
            for content in td :
                print(content.string)
                if(not isinstance(content.contents[0],str)) :
                    lhbUrl = content.contents[0].attrs["href"]
                    lhbUrl = lhbUrl.replace('cjmx','code')
                # print(lhbUrl)
                # lhbUrl='http://data.10jqka.com.cn/market/longhu/code/603871/'
                    CrawlerTHS.get_lhbData(CrawlerTHS.getLhbHtml(lhbUrl))

    def get_lhbData(html_text):
        # final = []
        bs = BeautifulSoup(html_text, "html.parser")
        rq = bs.find('input',attrs={"class" : "m_text_date startday", "type":"text"})
        print(rq.attrs['value'])
        body = bs.find("div", attrs={'class': 'rightcol fr'})
        stockconts = body.find_all("div", attrs={'class': 'stockcont'})
        for stockcont in stockconts:
            m_tables = stockcont.find_all("table", attrs={'class': 'm-table m-table-nosort mt10'})
            m_table_buy = m_tables[0]
            trs_buy = m_table_buy.find_all('tr')
            for tr_buy in trs_buy:
                bg_blue = ''
                if(len(tr_buy.attrs)>0):
                    bg_blue = tr_buy.attrs['class'][0]
                if(bg_blue != 'bg-blue') :
                    tds = tr_buy.find_all('td')
                    print(tds[0].contents[1].attrs['title'])
                    print(tds[1].string)
                    print(tds[2].string)
                    print(tds[3].string)
            m_table_sell = m_tables[1]

if __name__=='__main__':
    url = 'http://stock.10jqka.com.cn/'
    CrawlerTHS.get_data(CrawlerTHS.getHtml(url))
