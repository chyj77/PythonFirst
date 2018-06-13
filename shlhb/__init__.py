from apscheduler.schedulers.blocking import BlockingScheduler
from bs4 import BeautifulSoup
import time
import threading
import socket
import random
import requests
import http.client
import json
import pymongo

sched = BlockingScheduler()


# @sched.scheduled_job('interval', seconds=3)
# def timed_job():
#     print('This job is run every three minutes.')

@sched.scheduled_job('cron', day_of_week='mon-fri', hour='17', minute='30')
def scheduled_job():
    today = time.strftime('%Y%m%d', time.localtime(time.time()))
    startJob(today)

def startJob(today):
    print('This job is run every weekday at 5pm.')
    print('today = ',today)
    # begin_date = datetime.datetime.strptime('20180201', "%Y%m%d")
    # end_date = datetime.datetime.strptime(today,"%Y%m%d")
    # while begin_date <= end_date:
    #     date_str = begin_date.strftime("%Y%m%d")
    #     begin_date += datetime.timedelta(days=1)
    #     shurl = "http://query.sse.com.cn//marketdata/tradedata/queryAllTradeOpenDate.do?jsonCallBack=jsonpCallback60321&token=QUERY&tradeDate=%s&_=1519787646645"%(date_str)
    #     get_shdata(getshHtml(shurl),date_str)
    shurl = "http://query.sse.com.cn//marketdata/tradedata/queryAllTradeOpenDate.do?jsonCallBack=jsonpCallback60321&token=QUERY&tradeDate=2018301&_=1519787646645"
    get_shdata(getshHtml(shurl),today)

def getshHtml(url):
    header = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Cookie': 'yfx_c_g_u_id_10000042=_ck18022809153419407403135993657; yfx_mr_10000042=%3A%3Amarket_type_free_search%3A%3A%3A%3Abaidu%3A%3A%3A%3A%3A%3A%3A%3Awww.baidu.com%3A%3A%3A%3Apmf_from_free_search; yfx_mr_f_10000042=%3A%3Amarket_type_free_search%3A%3A%3A%3Abaidu%3A%3A%3A%3A%3A%3A%3A%3Awww.baidu.com%3A%3A%3A%3Apmf_from_free_search; yfx_key_10000042=; VISITED_COMPANY_CODE=%5B%22600000%22%2C%22600008%22%5D; VISITED_STOCK_CODE=%5B%22600000%22%2C%22600008%22%5D; seecookie=%5B600000%5D%3A%u6D66%u53D1%u94F6%u884C%2C%5B600008%5D%3A%u9996%u521B%u80A1%u4EFD; yfx_f_l_v_t_10000042=f_t_1519780534914__r_t_1519780534914__v_t_1519787853421__r_c_0; VISITED_MENU=%5B%229055%22%2C%228529%22%2C%228528%22%2C%228466%22%2C%228470%22%2C%228451%22%2C%228535%22%2C%228443%22%2C%229729%22%2C%2210883%22%2C%2210884%22%5D',
        'DNT': '1',
        'Host': 'query.sse.com.cn',
        'Referer': 'http://www.sse.com.cn/disclosure/diclosure/public/dailydata/',
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

def get_shdata(html_text,today):
    # final = []
    # today = time.strftime('%Y%m%d', time.localtime(time.time()))
    bs = BeautifulSoup(html_text, "html.parser")
    if(not bs.is_xml):
        print("找不到文件",time.strftime('%Y%m%d %H:%M:%S', time.localtime(time.time())))
        now = time.strftime('%Y%m%d', time.localtime(time.time()))
        if(today == now ):
            threading.Timer(60*2, startJob).start()
    else:
        jsonCallBack = bs.contents[0]
        jsonCallBack = jsonCallBack.replace('jsonpCallback60321(','')
        jsonCallBack = jsonCallBack[:-1]
        python_to_json = json.loads(jsonCallBack)
        pageHelp = python_to_json['pageHelp']
        if(pageHelp !=None):
            pageData = pageHelp['data']
            mongodata = {today : pageData}
            print(mongodata)
            # saveShMongoDB(mongodata)
        else:
            print("我等待了半小时",time.strftime('%Y%m%d %H:%M:%S', time.localtime(time.time())))
            now = time.strftime('%Y%m%d', time.localtime(time.time()))
            if(today == now ):
                threading.Timer(60*30, startJob).start()

def saveShMongoDB(mongodata):
    client = pymongo.MongoClient(host='*************', port=27017)
    db = client.mystock
    collection = db.shlhb
    collection.insert_one(mongodata)

if __name__ == '__main__':
    print('before the start funciton')
    sched.start()
    # scheduled_job()
    print("let us figure out the situation")
