from apscheduler.schedulers.blocking import BlockingScheduler
from bs4 import BeautifulSoup
import time
import datetime
import threading
import socket
import random
import requests
import http.client
import pymongo
from XinGu import XinGu
from xinguInfo import xinguInfo
from thsgn import Thsgn

sched = BlockingScheduler()


# @sched.scheduled_job('interval', seconds=3)
# def timed_job():
#     print('This job is run every three minutes.')

@sched.scheduled_job('cron', day_of_week='mon-fri', hour='17', minute='30',misfire_grace_time=1000)
def scheduled_job():
    today = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    startJob(today)


def startJob(today):
    print('This job is run every day at 5:30pm.', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    # today = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    print('today = ', today)
    today = "2018-04-19"
    begin_date = datetime.datetime.strptime('2015-01-07', "%Y-%m-%d")
    end_date = datetime.datetime.strptime(today,"%Y-%m-%d")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m-%d")
        begin_date += datetime.timedelta(days=1)
        shurl = "http://data.10jqka.com.cn/ifmarket/lhbggxq/report/%s/" % (date_str)
        get_data(getHtml(shurl),date_str)
    shurl = "http://data.10jqka.com.cn/ifmarket/lhbggxq/report/%s/" % (today)
    get_data(getHtml(shurl), today)


def getHtml(url):
    header = {
        'Accept': 'text/html, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Cookie': '__utmc=156575163; spversion=20130314; searchGuide=sg; __utma=156575163.2123015184.1517402961.1518405764.1518418592.3; __utmz=156575163.1518418592.3.3.utmcsr=10jqka.com.cn|utmccn=(referral)|utmcmd=referral|utmcct=/; historystock=1A0001%7C*%7C601519%7C*%7C000932%7C*%7C603516%7C*%7C002587; refreshStat=off; Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1519724465,1519813694,1519874903,1519874926; Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1=1519874926; Hm_lvt_60bad21af9c824a4a0530d5dbf4357ca=1519724465,1519813694,1519874903,1519874926; Hm_lpvt_60bad21af9c824a4a0530d5dbf4357ca=1519874926; Hm_lvt_f79b64788a4e377c608617fba4c736e2=1519724465,1519813694,1519874903,1519874926; Hm_lpvt_f79b64788a4e377c608617fba4c736e2=1519874926; v=AmVBANbaU_Ok8rcvr3k_phQKdCqbohrCY1P9pGdKIRyrfo8cL_IpBPOmDV30',
        'DNT': '1',
        'hexin-v': 'AmVBANbaU_Ok8rcvr3k_phQKdCqbohrCY1P9pGdKIRyrfo8cL_IpBPOmDV30',
        'Host': 'data.10jqka.com.cn',
        'Referer': 'http://data.10jqka.com.cn/market/longhu/',
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


def get_data(html_text, today):
    # final = []
    # today = time.strftime('%Y%m%d', time.localtime(time.time()))
    print("开始执行任务", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    bs = BeautifulSoup(html_text, "html.parser")
    leftcol = bs.find('div', attrs={'class': 'leftcol fl'})
    rightcol = bs.find('div', attrs={'class': 'rightcol fr'})

    lhbDataArray=[]
    if (leftcol != None):
        print("开始爬龙虎榜数据", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        pageData = leftcol.find("tbody")
        contents = pageData.contents
        i = 0
        for content in contents :
            lhbData = {}
            if( not isinstance(content ,str)):
                tds = content.contents
                lhbData['stockcont']=tds[1].text
                lhbData['stockcode']=tds[3].text
                lhbData['stockname']=tds[5].text
                lhbData['nowprice']=tds[7].text
                lhbData['zdf']=tds[9].text
                lhbData['cjje']=tds[11].text
                lhbData['jmre']=tds[13].text
                stockDetail = rightcol.find('div', attrs={'stockcode': tds[3].text})
                if(stockDetail == None ) :
                    continue
                lhbData['detail']=stockDetail.contents[1].text
                lhbData['cjDetail'] = stockDetail.contents[5].contents[1].text
                buyDetails = stockDetail.contents[5].contents[3]
                buyDetailss = buyDetails.find('tbody').find_all('tr')
                buyDetailArray = []
                for buyIndex in range(len(buyDetailss)):
                    buyDetail = {}
                    buyTr = buyDetailss[buyIndex]
                    buyDetail['yyb'] = buyTr.contents[1].text
                    buyDetail['mre'] = buyTr.contents[3].text
                    buyDetail['mce'] = buyTr.contents[5].text
                    buyDetail['jinge'] = buyTr.contents[7].text
                    buyDetailArray.append(buyDetail)
                lhbData['mryyb'] = buyDetailArray
                sellDetails = stockDetail.contents[5].contents[5]
                sellDetailss = sellDetails.find('tbody').find_all('tr')
                sellDetailArray = []
                for sellIndex in range(len(sellDetailss)):
                    sellDetail = {}
                    sellTr = sellDetailss[sellIndex]
                    sellDetail['yyb'] = sellTr.contents[1].text
                    sellDetail['mre'] = sellTr.contents[3].text
                    sellDetail['mce'] = sellTr.contents[5].text
                    sellDetail['jinge'] = sellTr.contents[7].text
                    sellDetailArray.append(sellDetail)
                lhbData['mcyyb'] = sellDetailArray
                lhbDataArray.append(lhbData)
                i = i+1
        mongodata = {"rq":today,"data":lhbDataArray}
        saveMongoDB(mongodata)
        print(mongodata)

    else:
        print("我等待了半小时", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        now = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        if(today == now ):
            print("重新执行任务", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
            threading.Timer(60 * 30, startJob).start()
        else:
            print("退出任务", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))


def saveMongoDB(mongodata):
    # client = pymongo.MongoClient(host='172.21.0.17', port=27017)
    client = pymongo.MongoClient(host='123.206.87.88', port=27017)
    db = client.mystock
    collection = db.thslhb
    collection.insert_one(mongodata)

@sched.scheduled_job('cron', day_of_week='mon-fri', hour='15', minute='30',misfire_grace_time=1000)
def scheduled_job1():
    today = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    print(today,"获取新股信息")
    XinGu.play(XinGu)

@sched.scheduled_job('cron', day_of_week='mon-fri', hour='16', minute='00',misfire_grace_time=1000)
def scheduled_job2():
    today = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    print(today ,"更新新股信息")
    xinguInfo.queryDB(xinguInfo)

@sched.scheduled_job('cron', day_of_week='sun', hour='12', minute='00',misfire_grace_time=1000)
def scheduled_job3():
    today = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    print(today ,"更新股票概念")
    Thsgn.play(Thsgn)

if __name__ == '__main__':
    # print('before the start thslhb funciton ---',time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    # sched.start()
    today = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    startJob(today)
    print("let us figure out the thslhb situation")
