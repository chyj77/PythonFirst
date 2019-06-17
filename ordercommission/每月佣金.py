# coding=gbk

import pymysql
import time
import math
from decimal import *

# 生产
# DB = pymysql.connect(host='rm-bp1d11o4l8p0u8923bo.mysql.rds.aliyuncs.com', port=3306, user='chenyongjin',
#                      password='AsDCDikb97', db='commission',
#                      charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
# uat
DB = pymysql.connect(host='bixuan-uat.bisinuolan.cn', port=3306, user='mysql',
                     password='dev_data2018', db='commission',
                     charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
# 测试
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


# 创建每个月的已提现表
def createOrderCommissionHis():
    cursor = DB.cursor()
    dropOrderCommissionHisSql = ' drop TABLE if EXISTS %s' % (commission_his_tablename)
    cursor.execute(dropOrderCommissionHisSql)
    createOrderCommissionHisSql = "CREATE TABLE %s (\
  `id` int(10) NOT NULL AUTO_INCREMENT,\
  `order_no` varchar(100) DEFAULT NULL COMMENT '订单号，取自order_info表中的order_sn|刁明昌|2019-01-11',\
  `order_id` varchar(100) DEFAULT NULL COMMENT '订单id，取自order_info表中的order_id|刁明昌|2019-01-11',\
  `user_id` varchar(100) DEFAULT NULL COMMENT '受益人，取自user_info表中的user_id|刁明昌|2019-01-11',\
  `commission_id` decimal(12,4) DEFAULT NULL COMMENT '佣金比例id|刁明昌|2019-01-11',\
  `commission_price` decimal(12,4) DEFAULT NULL COMMENT '佣金金额|刁明昌|2019-01-11',\
  `is_clearing` tinyint(1) DEFAULT '0' COMMENT '是否结算|@1:已结算@2:未结算|刁明昌|2019-01-11',\
  `arrival_time` datetime DEFAULT NULL COMMENT '实际到账时间|刁明昌|2019-01-11',\
  `status` tinyint(1) DEFAULT NULL COMMENT '状态|@1:有效@0:无效|刁明昌|2019-01-11',\
  `delete_flag` tinyint(1) NOT NULL DEFAULT '1' COMMENT '有效性|@1:正常@0:删除|刁明昌|2019-01-11',\
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间|刁明昌|2019-01-11',\
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间|刁明昌|2019-01-11',\
  `percentage_type` int(10) DEFAULT NULL COMMENT '提成类别 @ 0推荐董事，1直招董事，2间招董事，3推荐大区，4直招大区，5间招大区，6直招兰密，7间招兰密|陈永进|2019-01-11',\
  `goods_id` varchar(100) DEFAULT NULL COMMENT '商品id|刁明昌|2019-01-11',\
  `order_goods_id` varchar(100) DEFAULT NULL COMMENT '订单明细id|刁明昌|2019-01-11',\
  `sku_commission` decimal(12,4) DEFAULT NULL COMMENT '下单时的最大佣金|刁明昌|2019-01-11',\
  `remark` tinytext COMMENT '备注|刁明昌|2019-01-11',\
  `goods_name` varchar(255) DEFAULT '' COMMENT '商品名称|刁明昌|2019-01-11',\
  `real_payment` double(12,4) DEFAULT '0.00' COMMENT '实付金额|刁明昌|2019-01-11',\
  `goods_img` varchar(255) DEFAULT '' COMMENT '商品图片|刁明昌|2019-01-11',\
  `nickname` varchar(255) DEFAULT '' COMMENT '昵称|刁明昌|2019-01-11',\
  `order_time` datetime DEFAULT NULL COMMENT '下单时间|刁明昌|2019-01-11',\
  `order_info_user_id` varchar(255) DEFAULT NULL COMMENT '下单人id|刁明昌|2019-01-11',\
  `order_info_user_nickname` varchar(255) DEFAULT NULL COMMENT '下单人昵称|刁明昌|2019-01-11',\
  `system` varchar(255) DEFAULT NULL COMMENT '1@新系统 2@老系统|刁明昌|2019-01-11',\
  PRIMARY KEY (`id`) USING BTREE,\
  KEY `order_id` (`order_id`) USING BTREE,\
  KEY `user_id` (`user_id`) USING BTREE,\
  KEY `status` (`status`) USING BTREE,\
  KEY `order_time` (`order_time`) USING BTREE\
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='12月份订单佣金明细已提现表|刁明昌|2019-01-11'" % commission_his_tablename
    cursor.execute(createOrderCommissionHisSql)
    DB.commit()


# 将财务返回发放的佣金数据插入commission.order_commission_月份,更新实发和税
def updateCommission():
    updateCommissionSql = 'UPDATE %s  set actual_amount = ROUND(money *0.9,4),fee = ROUND(money *0.1,4)' % commission_tablename
    cursor = DB.cursor()
    print(updateCommissionSql)
    cursor.execute(updateCommissionSql)


# 根据commission.order_commission_月份 更新发放数据，withdraw表状态1
def updateWithDrawn():
    withDrawnCursor = DB.cursor()
    withDrawnSql = "INSERT into %s (withdraw_id,user_id,amount,fee,bank_name,actual_amount,withdraw_time,`month`,tax,bank_no,\
        `status`) select UUID(),id,money,fee,acountbank, actual_amount, moneydate,'%s',0.00,bankacount,1 \
          FROM %s " % (bsnl_withdraw, month, commission_tablename)
    print(withDrawnSql)
    withDrawnCursor.execute(withDrawnSql)
    # 转移佣金到这个表中 order_commission_his_
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
    #  清理旧数据已提现
    updateOldMatchTable = " update bsnl.old_match_table set undrawn_cash=0.00 where user_id in (SELECT id from %s) " % commission_tablename
    print(updateOldMatchTable)
    withDrawnCursor.execute(updateOldMatchTable)

    # 非箱起已提现转移到order_commission_his
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
    # 箱起已提现转移到order_commission_his
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
    # -- 将原未提现但在新月份提现的数据转移
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
    # -- 未提现表删除已提现数据
    deleteUnWithDrawSql = "delete from commission.order_commission_undraw_his \
 where user_id in (select  * from (select b.user_id from %s b where`month`='%s' and b.status=1)b)" % (bsnl_withdraw,month)
    print(deleteUnWithDrawSql)
    withDrawnCursor.execute(deleteUnWithDrawSql)

    DB.commit()


# 根据commission.order_commission_月份 更新未提现数据，withdraw表状态2
def updateUnWithDrawn():
    unWithDrawnCursor = DB.cursor()
    # 未提现数据
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
    # 未提现转移到order_commission_undraw_his
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
    # 删除旧数据
    delOldUnWithDrawnSql = "DELETE FROM %s WHERE	id IN \
        (	SELECT	id 	FROM	( SELECT a.id FROM %s a \
        JOIN %s b ON a.user_id = b.id \
        WHERE a.`status` = 2 ) x 	)" % (bsnl_withdraw, bsnl_withdraw, commission_tablename)
    print(delOldUnWithDrawnSql)
    unWithDrawnCursor.execute(delOldUnWithDrawnSql)

    DB.commit()


def refundCommission():
    refundCommissionCursor = DB.cursor()
    # 全额退款
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

    # 部分退款
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
    print('开始时间：', beginTime)
    print('结束时间：', endTime, " 耗时:", "%02d 小时 %02d 分钟 %02d 秒" % (h, m, s))
