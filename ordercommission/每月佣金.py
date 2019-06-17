# coding=gbk

import pymysql
import time
import math
from decimal import *

# ����
# DB = pymysql.connect(host='rm-bp1d11o4l8p0u8923bo.mysql.rds.aliyuncs.com', port=3306, user='chenyongjin',
#                      password='AsDCDikb97', db='commission',
#                      charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
# uat
DB = pymysql.connect(host='bixuan-uat.bisinuolan.cn', port=3306, user='mysql',
                     password='dev_data2018', db='commission',
                     charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
# ����
# DB = pymysql.connect(host='172.16.6.71', port=3306, user='mysql',
#                 password='dev_data2018', db='commission',
#                 charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

order_time = '201904'
month = '201905'
# order_time1 = '2019-03'

his_order_time = '201903'
# his_order_time1 = '2019-02'

order_tablename = 'commission.order_his_%s' % order_time

last_order_tablename = 'commission.order_his_%s' % his_order_time

caiwu_tablename = 'commission.tmp_order_caiwu_%s' % order_time

commission_tablename = 'commission.order_commission_%s' % order_time

commission_his_tablename = 'commission.order_commission_his_%s' % order_time

bsnl_withdraw = 'commission.withdraw'


# ����ÿ���µ������ֱ�
def createOrderCommissionHis():
    cursor = DB.cursor()
    dropOrderCommissionHisSql = ' drop TABLE if EXISTS %s' % (commission_his_tablename)
    cursor.execute(dropOrderCommissionHisSql)
    createOrderCommissionHisSql = "CREATE TABLE %s (\
  `id` int(10) NOT NULL AUTO_INCREMENT,\
  `order_no` varchar(100) DEFAULT NULL COMMENT '�����ţ�ȡ��order_info���е�order_sn|������|2019-01-11',\
  `order_id` varchar(100) DEFAULT NULL COMMENT '����id��ȡ��order_info���е�order_id|������|2019-01-11',\
  `user_id` varchar(100) DEFAULT NULL COMMENT '�����ˣ�ȡ��user_info���е�user_id|������|2019-01-11',\
  `commission_id` decimal(12,4) DEFAULT NULL COMMENT 'Ӷ�����id|������|2019-01-11',\
  `commission_price` decimal(12,4) DEFAULT NULL COMMENT 'Ӷ����|������|2019-01-11',\
  `is_clearing` tinyint(1) DEFAULT '0' COMMENT '�Ƿ����|@1:�ѽ���@2:δ����|������|2019-01-11',\
  `arrival_time` datetime DEFAULT NULL COMMENT 'ʵ�ʵ���ʱ��|������|2019-01-11',\
  `status` tinyint(1) DEFAULT NULL COMMENT '״̬|@1:��Ч@0:��Ч|������|2019-01-11',\
  `delete_flag` tinyint(1) NOT NULL DEFAULT '1' COMMENT '��Ч��|@1:����@0:ɾ��|������|2019-01-11',\
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '����ʱ��|������|2019-01-11',\
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '����ʱ��|������|2019-01-11',\
  `percentage_type` int(10) DEFAULT NULL COMMENT '������ @ 0�Ƽ����£�1ֱ�ж��£�2���ж��£�3�Ƽ�������4ֱ�д�����5���д�����6ֱ�����ܣ�7��������|������|2019-01-11',\
  `goods_id` varchar(100) DEFAULT NULL COMMENT '��Ʒid|������|2019-01-11',\
  `order_goods_id` varchar(100) DEFAULT NULL COMMENT '������ϸid|������|2019-01-11',\
  `sku_commission` decimal(12,4) DEFAULT NULL COMMENT '�µ�ʱ�����Ӷ��|������|2019-01-11',\
  `remark` tinytext COMMENT '��ע|������|2019-01-11',\
  `goods_name` varchar(255) DEFAULT '' COMMENT '��Ʒ����|������|2019-01-11',\
  `real_payment` double(12,4) DEFAULT '0.00' COMMENT 'ʵ�����|������|2019-01-11',\
  `goods_img` varchar(255) DEFAULT '' COMMENT '��ƷͼƬ|������|2019-01-11',\
  `nickname` varchar(255) DEFAULT '' COMMENT '�ǳ�|������|2019-01-11',\
  `order_time` datetime DEFAULT NULL COMMENT '�µ�ʱ��|������|2019-01-11',\
  `order_info_user_id` varchar(255) DEFAULT NULL COMMENT '�µ���id|������|2019-01-11',\
  `order_info_user_nickname` varchar(255) DEFAULT NULL COMMENT '�µ����ǳ�|������|2019-01-11',\
  `system` varchar(255) DEFAULT NULL COMMENT '1@��ϵͳ 2@��ϵͳ|������|2019-01-11',\
  PRIMARY KEY (`id`) USING BTREE,\
  KEY `order_id` (`order_id`) USING BTREE,\
  KEY `user_id` (`user_id`) USING BTREE,\
  KEY `status` (`status`) USING BTREE,\
  KEY `order_time` (`order_time`) USING BTREE\
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='12�·ݶ���Ӷ����ϸ�����ֱ�|������|2019-01-11'" % commission_his_tablename
    cursor.execute(createOrderCommissionHisSql)
    DB.commit()


# �����񷵻ط��ŵ�Ӷ�����ݲ���commission.order_commission_�·�,����ʵ����˰
def updateCommission():
    updateCommissionSql = 'UPDATE %s  set actual_amount = ROUND(money *0.9,4),fee = ROUND(money *0.1,4)' % commission_tablename
    cursor = DB.cursor()
    print(updateCommissionSql)
    cursor.execute(updateCommissionSql)


# ����commission.order_commission_�·� ���·������ݣ�withdraw��״̬1
def updateWithDrawn():
    withDrawnCursor = DB.cursor()
    withDrawnSql = "INSERT into %s (withdraw_id,user_id,amount,fee,bank_name,actual_amount,withdraw_time,`month`,tax,bank_no,\
        `status`) select UUID(),id,money,fee,acountbank, actual_amount, moneydate,'%s',0.00,bankacount,1 \
          FROM %s " % (bsnl_withdraw, month, commission_tablename)
    print(withDrawnSql)
    withDrawnCursor.execute(withDrawnSql)
    # ת��Ӷ��������� order_commission_his_
    insertCommissionSql = "INSERT into %s (\
        order_no,\
        order_id,\
        user_id,\
        commission_id,\
        commission_price,\
        is_clearing,\
        arrival_time,\
        status,\
        delete_flag,\
        created_at,\
        updated_at,\
        percentage_type,\
        goods_id,\
        order_goods_id,\
        sku_commission,\
        remark,\
        goods_name,\
        real_payment,\
        goods_img,\
        nickname,\
        order_time,\
        order_info_user_id,\
        order_info_user_nickname,\
        system)\
        select a.order_no,\
        a.order_id,\
        a.user_id,\
        a.commission_id,\
        a.commission_price,\
        a.is_clearing,\
        a.arrival_time,\
        a.status,\
        a.delete_flag,\
        a.created_at,\
        a.updated_at,\
        a.percentage_type,\
        a.goods_id,\
        a.order_goods_id,\
        a.sku_commission,\
        a.remark,\
        a.goods_name,\
        a.real_payment,\
        a.goods_img,\
        a.nickname,\
        a.order_time,\
        a.order_info_user_id,\
        a.order_info_user_nickname,\
        1\
        from bsnl.order_commission_info a \
        join %s b on a.order_id = b.order_id  and b.`status` in (2,3,4,8,10)\
        and DATE_FORMAT(b.created_at,'%%Y%%m')='%s'\
        where DATE_FORMAT(a.order_time,'%%Y%%m')='%s' " % (
        commission_his_tablename, order_tablename, order_time, order_time)
    print(insertCommissionSql)
    withDrawnCursor.execute(insertCommissionSql)
    #  ���������������
    updateOldMatchTable = " update bsnl.old_match_table set undrawn_cash=0.00 where user_id in (SELECT id from %s) " % commission_tablename
    print(updateOldMatchTable)
    withDrawnCursor.execute(updateOldMatchTable)

    # ������������ת�Ƶ�order_commission_his
    insertWithDrawnSql = "INSERT into commission.order_commission_his ( \
         order_no, \
         order_id, \
         user_id, \
         commission_id, \
         commission_price, \
         is_clearing, \
         arrival_time, \
         status, \
         delete_flag, \
         created_at, \
         updated_at, \
         percentage_type, \
         goods_id, \
         order_goods_id, \
         sku_commission, \
         remark, \
         goods_name, \
         real_payment, \
         goods_img, \
         nickname, \
         order_time, \
         order_info_user_id, \
         order_info_user_nickname, \
         date) \
         select a.order_no, \
          a.order_id, \
          a.user_id, \
          a.commission_id, \
          a.commission_price, \
          a.is_clearing, \
          a.arrival_time, \
          a.status, \
          a.delete_flag, \
          a.created_at, \
          a.updated_at, \
          a.percentage_type, \
          a.goods_id, \
          a.order_goods_id, \
          a.sku_commission, \
          a.remark, \
          a.goods_name, \
          a.real_payment, \
          a.goods_img, \
          a.nickname, \
          a.order_time, \
          a.order_info_user_id, \
          a.order_info_user_nickname, \
          '%s' \
         from bsnl.order_commission_info a \
         join %s b on a.order_id = b.order_id  and b.`status` in (3,4) and b.type not in (9,11) and DATE_FORMAT(b.created_at,'%%Y%%m')='%s' \
         where DATE_FORMAT(a.order_time,'%%Y%%m')='%s'  and a.user_id in (SELECT id from %s)" \
                         % (order_time, order_tablename, order_time, order_time, commission_tablename)
    withDrawnCursor.execute(insertWithDrawnSql)
    print(insertWithDrawnSql)
    # ����������ת�Ƶ�order_commission_his
    insertBoxWithDrawnSql = "INSERT into commission.order_commission_his ( \
                 order_no, \
                 order_id, \
                 user_id, \
                 commission_id, \
                 commission_price, \
                 is_clearing, \
                 arrival_time, \
                 status, \
                 delete_flag, \
                 created_at, \
                 updated_at, \
                 percentage_type, \
                 goods_id, \
                 order_goods_id, \
                 sku_commission, \
                 remark, \
                 goods_name, \
                 real_payment, \
                 goods_img, \
                 nickname, \
                 order_time, \
                 order_info_user_id, \
                 order_info_user_nickname, \
                 date) \
                 select a.order_no, \
                  a.order_id, \
                  a.user_id, \
                  a.commission_id, \
                  a.commission_price, \
                  a.is_clearing, \
                  a.arrival_time, \
                  a.status, \
                  a.delete_flag, \
                  a.created_at, \
                  a.updated_at, \
                  a.percentage_type, \
                  a.goods_id, \
                  a.order_goods_id, \
                  a.sku_commission, \
                  a.remark, \
                  a.goods_name, \
                  a.real_payment, \
                  a.goods_img, \
                  a.nickname, \
                  a.order_time, \
                  a.order_info_user_id, \
                  a.order_info_user_nickname, \
                  '%s' \
                 from bsnl.order_commission_info a \
                 join %s b on a.order_id = b.order_id  and b.`status` in (2,3,4,10) and b.type  in (9,11) and DATE_FORMAT(b.created_at,'%%Y%%m')='%s' \
                 where DATE_FORMAT(a.order_time,'%%Y%%m')='%s'  and a.user_id in (SELECT id from %s)" \
                            % (order_time, order_tablename, order_time, order_time, commission_tablename)
    print(insertBoxWithDrawnSql)
    withDrawnCursor.execute(insertBoxWithDrawnSql)
    # -- ��ԭδ���ֵ������·����ֵ�����ת��
    insertHasWithDrawnSql = "INSERT into commission.order_commission_his ( \
        order_no, \
        order_id, \
        user_id, \
        commission_id, \
        commission_price, \
        is_clearing, \
        arrival_time, \
        status, \
        delete_flag, \
        created_at, \
        updated_at, \
        percentage_type, \
        goods_id, \
        order_goods_id, \
        sku_commission, \
        remark, \
        goods_name, \
        real_payment, \
        goods_img, \
        nickname, \
        order_time, \
        order_info_user_id, \
        order_info_user_nickname, \
        date) \
        select a.order_no, \
         a.order_id, \
         a.user_id, \
         a.commission_id, \
         a.commission_price, \
         a.is_clearing, \
         a.arrival_time, \
         a.status, \
         a.delete_flag, \
         a.created_at, \
         a.updated_at, \
         a.percentage_type, \
         a.goods_id, \
         a.order_goods_id, \
         a.sku_commission, \
         a.remark, \
         a.goods_name, \
         a.real_payment, \
         a.goods_img, \
         a.nickname, \
         a.order_time, \
         a.order_info_user_id, \
         a.order_info_user_nickname, \
         '%s' \
        from commission.order_commission_undraw_his a \
        join withdraw b on a.user_id = b.user_id \
        where b.`month`='%s' and b.status=1" % (order_time, month)
    print(insertHasWithDrawnSql)
    withDrawnCursor.execute(insertHasWithDrawnSql)
    # -- δ���ֱ�ɾ������������
    deleteUnWithDrawSql = "delete from commission.order_commission_undraw_his \
 where user_id in (select  * from (select b.user_id from %s b where`month`='%s' and b.status=1)b)" % (bsnl_withdraw,month)
    print(deleteUnWithDrawSql)
    withDrawnCursor.execute(deleteUnWithDrawSql)

    DB.commit()


# ����commission.order_commission_�·� ����δ�������ݣ�withdraw��״̬2
def updateUnWithDrawn():
    unWithDrawnCursor = DB.cursor()
    # δ��������
    unWithDrawnSql = "INSERT INTO %s ( withdraw_id, user_id, amount, withdraw_time, `month`, `status` ) \
         SELECT \
         UUID( ) withdraw_id,\
         a.userid user_id,\
         a.commission amount,\
         null withdraw_time,\
         '%s' `month`,\
         2 `status` \
         FROM %s a \
          where a.userid not in (SELECT id from %s)\
          and a.commission!=0 " % (bsnl_withdraw, month, caiwu_tablename, commission_tablename)
    print(unWithDrawnSql)
    unWithDrawnCursor.execute(unWithDrawnSql)
    # δ����ת�Ƶ�order_commission_undraw_his
    unHisWithDrawnSql = "INSERT into commission.order_commission_undraw_his (\
            order_no,\
            order_id,\
            user_id,\
            commission_id,\
            commission_price,\
            is_clearing,\
            arrival_time,\
            status,\
            delete_flag,\
            created_at,\
            updated_at,\
            percentage_type,\
            goods_id,\
            order_goods_id,\
            sku_commission,\
            remark,\
            goods_name,\
            real_payment,\
            goods_img,\
            nickname,\
            order_time,\
            order_info_user_id,\
            order_info_user_nickname,\
            system)\
            select a.order_no,\
            a.order_id,\
            a.user_id,\
            a.commission_id,\
            a.commission_price,\
            a.is_clearing,\
            a.arrival_time,\
            a.status,\
            a.delete_flag,\
            a.created_at,\
            a.updated_at,\
            a.percentage_type,\
            a.goods_id,\
            a.order_goods_id,\
            a.sku_commission,\
            a.remark,\
            a.goods_name,\
            a.real_payment,\
            a.goods_img,\
            a.nickname,\
            a.order_time,\
            a.order_info_user_id,\
            a.order_info_user_nickname,\
            1 system\
            from %s a\
            where a.user_id not in (SELECT b.user_id from withdraw b\
            WHERE  `month`='%s'  and b.status=1)\
            and (a.user_id,a.order_id) not in (select user_id,order_id from commission.order_commission_undraw_his)" % (
                    commission_his_tablename, month)
    print(unHisWithDrawnSql)
    unWithDrawnCursor.execute(unHisWithDrawnSql)
    # ɾ��������
    delOldUnWithDrawnSql = "DELETE FROM %s WHERE	id IN \
        (	SELECT	id 	FROM	( SELECT a.id FROM %s a \
        JOIN %s b ON a.user_id = b.id \
        WHERE a.`status` = 2 ) x 	)" % (bsnl_withdraw, bsnl_withdraw, commission_tablename)
    print(delOldUnWithDrawnSql)
    unWithDrawnCursor.execute(delOldUnWithDrawnSql)

    DB.commit()


def refundCommission():
    refundCommissionCursor = DB.cursor()
    # ȫ���˿�
    allRefundCommissonSql = "INSERT into commission.order_commission_chargeback (\
            order_no,\
            order_id,\
            user_id, \
            commission_id,\
            commission_price, \
            is_clearing,  \
            arrival_time, \
            status,  \
            delete_flag,  \
            created_at,\
            updated_at,\
            percentage_type,  \
            goods_id,\
            order_goods_id,\
            sku_commission,\
            remark,  \
            goods_name,\
            real_payment, \
            goods_img, \
            nickname,\
            order_time,\
            order_info_user_id,\
            order_info_user_nickname,\
            goods_number, \
            package_number,\
            date) \
            SELECT\
            a.order_no,\
            a.order_id,\
            a.user_id, \
            a.commission_id,  \
            a.commission_price*-1,\
            a.is_clearing,\
            a.arrival_time,\
            a.status,\
            a.delete_flag,\
            a.created_at, \
            a.updated_at, \
            a.percentage_type,\
            a.goods_id,\
            a.order_goods_id, \
            a.sku_commission, \
            a.remark,\
            a.goods_name, \
            a.real_payment,\
            a.goods_img,  \
            a.nickname,\
            a.order_time, \
            a.order_info_user_id, \
            a.order_info_user_nickname, \
            ifnull(a.goods_number,0),\
            a.package_number, \
            '%s' \
             FROM bsnl.order_commission_info a join bsnl.order_info b\
            on a.order_id = b.order_id and b.`status`=8\
            where DATE_FORMAT(a.order_time,'%%Y%%m')='%s'  \
            and DATE_FORMAT(b.created_at,'%%Y%%m')='%s'" % (order_time, order_time, order_time)
    print(allRefundCommissonSql)
    refundCommissionCursor.execute(allRefundCommissonSql)

    # �����˿�
    partRefundCommissonSql = "INSERT into commission.order_commission_chargeback_section (\
        order_no,\
        order_id,\
        user_id, \
        commission_id,\
        commission_price, \
        is_clearing,  \
        arrival_time, \
        status,  \
        delete_flag,  \
        created_at,\
        updated_at,\
        percentage_type,  \
        goods_id,\
        order_goods_id,\
        sku_commission,\
        remark,  \
        goods_name,\
        real_payment, \
        goods_img, \
        nickname,\
        order_time,\
        order_info_user_id,\
        order_info_user_nickname,\
        goods_number, \
        package_number,\
        date) \
        SELECT\
        a.order_no,\
        a.order_id,\
        a.user_id, \
        a.commission_id,  \
        a.commission_price,\
        a.is_clearing,\
        a.arrival_time,\
        a.status,\
        a.delete_flag,\
        a.created_at, \
        a.updated_at, \
        a.percentage_type,\
        a.goods_id,\
        a.order_goods_id, \
        a.sku_commission, \
        a.remark,\
        a.goods_name, \
        a.real_payment,\
        a.goods_img,  \
        a.nickname,\
        a.order_time, \
        a.order_info_user_id, \
        a.order_info_user_nickname, \
        ifnull(b.goods_number,0),\
        b.package_number, \
        DATE_FORMAT(a.order_time,'%Y%m') \
         FROM bsnl.order_commission_change a join bsnl.order_goods_refund b\
        on a.order_id = b.order_id and a.goods_id = b.goods_id and b.is_use=1\
        where  a.created_at > (SELECT max(updated_at) from commission.order_commission_chargeback_section )"
    print(partRefundCommissonSql)
    refundCommissionCursor.execute(partRefundCommissonSql)

    DB.commit()


if __name__ == '__main__':
    begin = time.time()
    beginTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    # createOrderCommissionHis()
    # updateWithDrawn()
    updateUnWithDrawn()
    refundCommission()
    total = (time.time() - begin)
    endTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    m, s = divmod(total, 60)
    h, m = divmod(m, 60)
    print('��ʼʱ�䣺', beginTime)
    print('����ʱ�䣺', endTime, " ��ʱ:", "%02d Сʱ %02d ���� %02d ��" % (h, m, s))
