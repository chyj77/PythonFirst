# coding=gbk

import openpyxl
import pymysql
import time

# ����
DB = pymysql.connect(host='rm-bp1d11o4l8p0u8923bo.mysql.rds.aliyuncs.com', port=3306, user='chenyongjin',
                     password='AsDCDikb97', db='commission',
                     charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
# uat
# DB = pymysql.connect(host='bixuan-uat.bisinuolan.cn', port=3306, user='mysql',
#                      password='dev_data2018', db='commission',
#                      charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
# ����
# DB = pymysql.connect(host='172.16.6.71', port=3306, user='mysql',
#                      password='dev_data2018', db='commission',
#                      charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

path = r"C:\Users\hailxie\Documents\WeChat Files\chyj_1977\FileStorage\File\2019-06\6.17��ƥ�䶭����Ϣ(1).xlsx"


def readexcel():
    cursor = DB.cursor()
    inwb = openpyxl.load_workbook(path)  # ���ļ�
    sheetnames = inwb.get_sheet_names()  # ��ȡ���ļ������е�sheet��ͨ�����ֵķ�ʽ
    sheet = inwb.get_sheet_by_name(sheetnames[0])  # ��ȡ��һ��sheet����

    # ��ȡsheet���������������
    rows = sheet.max_row + 1
    cols = sheet.max_column
    print("�������=", sheet.max_row)
    print("�������=", cols)
    for r in range(2, rows):
        persentordernos = sheet.cell(r, 3).value
        print(persentordernos)
        if persentordernos is not None :
            persentorderno = persentordernos.split(",")
            sql = "select b.real_name,b.mobile from bsnl.order_info a,bsnl.user_info b where a.user_id = b.user_id and  order_no='%s' limit 1" % (persentorderno[0])
            print(sql)
            cursor.execute(sql)
            persent = cursor.fetchone();
            sheet.cell(row=r, column=6).value = persent['real_name']
            sheet.cell(row=r, column=7).value = persent['mobile']

    inwb.save(filename=path)

if __name__ == '__main__':
    begin = time.time()
    beginTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    readexcel()
    total = (time.time() - begin)
    endTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    m, s = divmod(total, 60)
    h, m = divmod(m, 60)
    print('��ʼʱ�䣺', beginTime)
    print('����ʱ�䣺', endTime, " ��ʱ:", "%02d Сʱ %02d ���� %02d ��" % (h, m, s))
