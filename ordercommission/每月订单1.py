# coding=gbk

import pymysql
import time
import math
from decimal import *

# 生产
DB = pymysql.connect(host='rm-bp1d11o4l8p0u8923bo.mysql.rds.aliyuncs.com', port=3306, user='chenyongjin',
                     password='AsDCDikb97', db='commission',
                     charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
# uat
# DB = pymysql.connect(host='bixuan-uat.bisinuolan.cn', port=3306, user='mysql',
#                      password='dev_data2018', db='commission',
#                      charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
# 测试
# DB = pymysql.connect(host='172.16.6.71', port=3306, user='mysql',
#                      password='dev_data2018', db='commission',
#                      charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

order_time = '201905'

his_order_time = '201904'

order_tablename = 'order_his_%s'%order_time

last_order_tablename = 'order_his_%s'%his_order_time

commission_tablename = 'tmp_order_caiwu_%s'%order_time


def monthOrder():
    schemaTable_sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'commission' and TABLE_NAME = '%s'" % (
        order_tablename)
    schemaTable_cursor = DB.cursor()
    schemaTable_cursor.execute(schemaTable_sql)
    schemaTable_data = schemaTable_cursor.fetchone()
    if schemaTable_data == None:
        create_order_sql = "CREATE TABLE `commission`.%s (\
        `id` int(10) NOT NULL AUTO_INCREMENT,\
        `user_id` varchar(100) NOT NULL COMMENT '用户id，取自user_info表user_id|刁明昌|2018-06-29',\
        `user_role` int(1) DEFAULT NULL COMMENT '用户购买时的等级',\
        `order_id` varchar(100) DEFAULT '' COMMENT '订单ID|陈永进|2018-07-03',\
        `order_no` varchar(100) NOT NULL COMMENT '订单号|刁明昌|2018-06-29',\
        `country` varchar(100) DEFAULT '' COMMENT '收货人国家|刁明昌|2018-06-29',\
        `last_sync_time` datetime DEFAULT NULL COMMENT '最后同步时间|刁明昌|2018-06-29',\
        `e3_order_no` varchar(100) DEFAULT '' COMMENT 'E3订单编号|刁明昌|2018-06-29',\
        `status` int(10) DEFAULT '0' COMMENT '订单状态（待付款，待发货，待收货，已收货，已取消，付款异常，超时取消，已退款）|@1:待付款@2:待发货@3:待收货@4:已收货@5:已取消@6:付款异常@7:超时取消@8:已退款|刁明昌|2018-06-29',\
        `region_code` varchar(255) DEFAULT NULL COMMENT '手机区号', \
         `mobile` varchar(50) DEFAULT '' COMMENT '收货人手机号|刁明昌|2018-06-29',\
        `consignee` varchar(100) DEFAULT '' COMMENT '收货人姓名|刁明昌|2018-06-29',\
        `address` varchar(255) DEFAULT '' COMMENT '收货人详细地址|刁明昌|2018-06-29',\
        `district` varchar(100) DEFAULT NULL COMMENT '收货人地区|刁明昌|2018-06-29',\
        `real_pay_price` decimal(10,2) DEFAULT NULL COMMENT '实际付款金额|刁明昌|2018-06-29',\
        `goods_amount` decimal(10,2) DEFAULT NULL COMMENT '商品总金额|刁明昌|2018-06-29',\
        `status_info` int(10) DEFAULT '1' COMMENT '订单状态（申请中，审核通过，审核不通过，E3处理中，E3处理成功，E3处理失败，退货入库成功，退货入库失败，审核物流状态中，审核物流状态成功，审核物流状态失败，退款处理中，退款成功，退款失败）|@1:申请中@2:审核通过@3:审核不通过@4:E3处理中@5:E3处理成功@6:E3处理失败@7:退货入库成功@8:退货入库失败@9:审核物流状态中@10:审核物流状态成功@11:审核物流状态失败@12:退款处理中@13:退款成功@14:退款失败|刁明昌|2018-06-29',\
        `city` varchar(100) DEFAULT '' COMMENT '收货人城市|刁明昌|2018-06-29',\
        `province` varchar(100) DEFAULT '' COMMENT '收货人省份|刁明昌|2018-06-29',\
        `type` int(10) DEFAULT NULL COMMENT '订单类别|@1:普通@7:会员礼包@3:会员组合礼包@8:大区礼包@9 城市合伙人|刁明昌|2018-06-27',\
        `order_goods_number` int(2) DEFAULT '1' COMMENT '订单商品数量',\
        `remark` varchar(1000) DEFAULT '' COMMENT '备注',\
        `coupon_price` decimal(10,2) DEFAULT NULL COMMENT '使用的优惠券总价格',\
        `coupon_flag` tinyint(1) DEFAULT '0' COMMENT '是否使用优惠券|@0:没有使用@1：使用',\
        `user_coupon_ids` varchar(255) DEFAULT NULL COMMENT '用户使用的优惠券id,多个优惠券用’，‘分开',\
        `postage` decimal(10,2) DEFAULT NULL COMMENT '邮费',\
        `activity_id` varchar(100) DEFAULT NULL COMMENT '活动ID',\
        `save_by_activity_price` decimal(10,2) DEFAULT NULL COMMENT '活动节省的价格',\
        `save_by_member_price` decimal(10,2) DEFAULT NULL COMMENT '会员节省的价格',\
        `invoice_status` tinyint(1) DEFAULT NULL COMMENT '发票状态：0不可开票1可开票2已开票',\
        `pay_channel` varchar(16) DEFAULT NULL COMMENT '支付渠道',\
        `delete_flag` tinyint(1) NOT NULL DEFAULT '1' COMMENT '有效性|@1:正常@0:删除|刁明昌|2018-06-29',\
        `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间|刁明昌|2018-06-29',\
        `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间|刁明昌|2018-06-29',\
        `level_reduction` decimal(10,2) DEFAULT NULL COMMENT '等级减免金额|陈永进|2018-07-02',\
        `store_partner_role` tinyint(1) DEFAULT NULL COMMENT '门店合伙人角色@1普通，@2门店大区，@3门店合伙人',\
        PRIMARY KEY (`id`) USING BTREE,\
        UNIQUE KEY `order_no` (`order_no`) USING BTREE,\
        KEY `user_id` (`user_id`) USING BTREE,\
        KEY `e3_order_no` (`e3_order_no`) USING BTREE,\
        KEY `orderinfo_order_id` (`order_id`) USING BTREE,\
        KEY `order_createdAt` (`created_at`) USING BTREE,\
        KEY `order_info_user_status_index` (`user_id`,`status`),\
        KEY `order_info_status_index` (`status`),\
        KEY `order_info_type_index` (`type`)\
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='每月临时订单信息表|陈永进|2019-03-29'" % (
            order_tablename)
        cursor = DB.cursor()
        cursor.execute(create_order_sql)
    else:
        truncate_sql = "truncate table `commission`.%s" % (order_tablename)
        truncate_cursor = DB.cursor()
        truncate_cursor.execute(truncate_sql)


def transOrder():
    transOrder_sql = "insert into `commission`.%s (`user_id`, `user_role`, `order_id`, \
                     `order_no`, `country`, `last_sync_time`,`e3_order_no`, `status`, `region_code`, \
                     `mobile`, `consignee`, `address`,`district`, `real_pay_price`, `goods_amount`, \
                     `status_info`, `city`, `province`,`type`, `order_goods_number`, `remark`, `coupon_price`,\
                    `coupon_flag`, `user_coupon_ids`, `postage`, `activity_id`, `save_by_activity_price`, \
                    `save_by_member_price`,`invoice_status`, `pay_channel`, `delete_flag`, `created_at`, \
                    `updated_at`, `level_reduction`, `store_partner_role`)\
                    SELECT \
                    `user_id`, `user_role`, `order_id`, `order_no`, `country`, `last_sync_time`,\
                    `e3_order_no`, `status`, `region_code`, `mobile`, `consignee`, `address`, `district`, `real_pay_price`,\
                    `goods_amount`, `status_info`, `city`, `province`, `type`, `order_goods_number`, `remark`, `coupon_price`,\
                    `coupon_flag`, `user_coupon_ids`, `postage`, `activity_id`, `save_by_activity_price`, `save_by_member_price`,\
                    `invoice_status`, `pay_channel`, `delete_flag`, `created_at`, `updated_at`, `level_reduction`, `store_partner_role`\
                    from bsnl.order_info\
                    where `status` in (2,3,4,8,10) and DATE_FORMAT(created_at, '%%Y%%m') =   '%s'" % (
    order_tablename, order_time)
    # print(transOrder_sql)
    transOrder_cursor = DB.cursor()
    i = transOrder_cursor.execute(transOrder_sql)
    DB.commit()
    print(i)

def createCommissionTable():
    schemaTable_sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'commission' and TABLE_NAME = '%s'" % (
        commission_tablename)
    schemaTable_cursor = DB.cursor()
    schemaTable_cursor.execute(schemaTable_sql)
    schemaTable_data = schemaTable_cursor.fetchone()
    if schemaTable_data == None:
        create_commission_sql = "CREATE TABLE `commission`.%s (\
        `userid` varchar(50) CHARACTER SET utf8mb4 NOT NULL,\
        `realName` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `appMobile` varchar(100) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `cardNo` varchar(30) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `idNo` varchar(30) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `role` varchar(10) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `bank` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `commission` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `renStatus` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `userStatus` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `bankOfDeposit` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `companyName` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `cardOfUser` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `bankMobile` varchar(100) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `companyAccount` varchar(30) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `lastCommission` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `drawn` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `unDrawn` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `totalDrawn` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        `isHalfDirector` varchar(10) COLLATE utf8mb4_general_ci DEFAULT NULL,\
        PRIMARY KEY (`userid`)\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci  COMMENT='每月临时佣金信息表|陈永进|2019-03-29'" % (
            commission_tablename)
        cursor = DB.cursor()
        cursor.execute(create_commission_sql)
    else:
        truncate_sql = "truncate table `commission`.%s" % (commission_tablename)
        truncate_cursor = DB.cursor()
        truncate_cursor.execute(truncate_sql)

def commission():
    user_sql = "SELECT DISTINCT user_id,`mobile`,`role`,`delete_flag`,`real_name` FROM  bsnl.user_info " \
               "where  role in (2,3)  order by user_id limit 40000,40000"
    user_cursor = DB.cursor()
    user_cursor.execute(user_sql)
    user_datas = user_cursor.fetchall()
    print('总数：',user_cursor.rowcount)
    for user_data in user_datas:
        userId = user_data['user_id']
        mobile = user_data['mobile']
        role = int(user_data['role'])
        deleteFlag = int(user_data['delete_flag'])
        realName = user_data['real_name']
        user_status = ""
        if deleteFlag == 1:
            user_status = "有效"
        else:
            user_status = "无效"
        company_name = ''
        bank_of_deposit = ''
        company_account = ''
        card_no = ""
        bank = ""
        bankmobile=""
        id_number = ""
        status = 0
        bankRealName=""

        user_role = ""
        if role == 2:
            user_role = "大区"
        elif role == 3:
            user_role = "董事"
        else:
            user_role = '碧选会员'

        ren_status = ""
        if role == 3:
            bsnl_agent_bank_card_sql = "select `company_name`,`bank_of_deposit`,`company_account`,`user_name` from bsnl.agent_bank_card where user_id='%s'" % (userId)
            bsnl_agent_bank_card_cursor = DB.cursor()
            bsnl_agent_bank_card_cursor.execute(bsnl_agent_bank_card_sql)
            bsnl_user_info_datas = bsnl_agent_bank_card_cursor.fetchone()
            if bsnl_user_info_datas != None:
                company_name = bsnl_user_info_datas['company_name']
                bank_of_deposit = bsnl_user_info_datas['bank_of_deposit']
                company_account = bsnl_user_info_datas['company_account']
                bankRealName = bsnl_user_info_datas['user_name']
        else:
            bsnl_user_bank_card_sql = "select `card_no`,`bank`,`id_number`,`status`,`mobile`,`real_name` from bsnl.user_bank_card where status=3 and user_id='%s' order by updated_at desc limit 1" % (userId)
            bsnl_user_bank_card_cursor = DB.cursor()
            bsnl_user_bank_card_cursor.execute(bsnl_user_bank_card_sql)
            bsnl_user_bank_card_datas = bsnl_user_bank_card_cursor.fetchone()
            if bsnl_user_bank_card_datas != None:
                card_no = bsnl_user_bank_card_datas['card_no']
                bank = bsnl_user_bank_card_datas['bank']
                id_number = bsnl_user_bank_card_datas['id_number']
                status = int(bsnl_user_bank_card_datas['status'])
                bankmobile = bsnl_user_bank_card_datas['mobile']
                bankRealName = bsnl_user_bank_card_datas['real_name']
                if status == 3:
                    ren_status = "已认证"
                else:
                    ren_status = "认证失败"
# 是否半价董事
        bsnl_user_half_sql="select half_director from bsnl.user_info_extends where half_director=1 and user_id = '%s' limit 1"%(userId)
        bsnl_user_half_cursor = DB.cursor()
        bsnl_user_half_cursor.execute(bsnl_user_half_sql)
        bsnl_user_half_datas = bsnl_user_half_cursor.fetchone()
        halfDirector = '否'
        if  bsnl_user_half_datas != None:
            halfDirector = '是'

        # 当月预发的佣金状态为已发货，已收货(不含箱起)
        bsnl_user_commission_sql = "SELECT SUM(commission_price) commissionPrice from bsnl.order_commission_info a join commission.%s b on a.order_id = b.order_id" \
                                   "  where b.status in (3,4) and b.type not in (9,11) and a.user_id = '%s' " % (order_tablename,userId)
        bsnl_user_commission_cursor = DB.cursor()
        bsnl_user_commission_cursor.execute(bsnl_user_commission_sql)
        bsnl_user_commission_datas = bsnl_user_commission_cursor.fetchone()

        commission = Decimal(0.00)
        yongjin = Decimal(0.00)
        if bsnl_user_commission_datas==None or bsnl_user_commission_datas['commissionPrice'] == None:
            yongjin = Decimal(0.00)
        else:
            yongjin = Decimal(bsnl_user_commission_datas['commissionPrice'])
        # commission = yongjin
        # 当月预发的佣金状态为已发货，已收货(含箱起)
        bsnl_user_boxcommission_sql = "SELECT SUM(commission_price) commissionPrice from bsnl.order_commission_info a join commission.%s b on a.order_id = b.order_id" \
                                   "  where b.status in (2,3,4,10) and b.type in (9,11) and a.user_id = '%s' " % (order_tablename,userId)
        bsnl_user_boxcommission_cursor = DB.cursor()
        bsnl_user_boxcommission_cursor.execute(bsnl_user_boxcommission_sql)
        bsnl_user_boxcommission_datas = bsnl_user_boxcommission_cursor.fetchone()

        boxyongjin = Decimal(0.00)
        if bsnl_user_boxcommission_datas==None or bsnl_user_boxcommission_datas['commissionPrice'] == None:
            boxyongjin = Decimal(0.00)
        else:
            boxyongjin = Decimal(bsnl_user_boxcommission_datas['commissionPrice'])

        commission = yongjin + boxyongjin
        # 上月待发货未发佣金，本月状态已改为已发货，已收货
        bsnl_user_lastcommission_sql = "SELECT SUM(commission_price) commissionPrice from bsnl.order_commission_info a join commission.%s b on a.order_id = b.order_id" \
                                    " join bsnl.order_info c on b.order_id=c.order_id "\
                                   "  where b.status in (2,10) and c.status in (3,4) and c.type not in (9,11) and a.user_id = '%s'" % (last_order_tablename,userId)
        bsnl_user_lastcommission_cursor = DB.cursor()
        bsnl_user_lastcommission_cursor.execute(bsnl_user_lastcommission_sql)
        bsnl_user_lastcommission_datas = bsnl_user_lastcommission_cursor.fetchone()

        lastcommission = Decimal(0.00)
        if bsnl_user_lastcommission_datas==None or bsnl_user_lastcommission_datas['commissionPrice'] == None:
            lastcommission = Decimal(0.00)
        else:
            lastcommission = Decimal(bsnl_user_lastcommission_datas['commissionPrice'])
        commission = lastcommission + commission

        # if userId=='de38e836-be87-4821-a670-ad181f00d78a':
        #     commission = commission -  Decimal(0.75)
        # if userId=='27d60282-d0b4-40ab-ae8c-1b7abeabf9a1':
        #     commission = commission + Decimal(0.75)

        # 本月待发货未发佣金
        bsnl_user_selfCommission_sql = "SELECT SUM(commission_price) commissionPrice from bsnl.order_commission_info a join commission.%s b on a.order_id = b.order_id" \
                                        "  where b.status in (2,10) and b.type not in (9,11)  and a.user_id = '%s'" % (order_tablename,userId)
        bsnl_user_selfCommission_cursor = DB.cursor()
        bsnl_user_selfCommission_cursor.execute(bsnl_user_selfCommission_sql)
        bsnl_user_selfCommission_datas = bsnl_user_selfCommission_cursor.fetchone()
        selCommission= Decimal(0.00)
        if bsnl_user_selfCommission_datas==None or bsnl_user_selfCommission_datas['commissionPrice'] == None:
            selCommission = Decimal(0.00)
        else:
            selCommission = Decimal(bsnl_user_selfCommission_datas['commissionPrice'])
        # 退货退款
        bsnl_user_change_sql = "select a.order_id orderRefundId, sum(ifnull(commission_price,0.00)) unwithdrawn \
                                from bsnl.order_commission_change a\
                                where 1=1  and user_id ='%s'\
                                and DATE_FORMAT(created_at,'%%Y%%m')=%s group by a.order_id" % (userId,order_time)
        bsnl_user_change_cursor = DB.cursor()
        bsnl_user_change_cursor.execute(bsnl_user_change_sql)
        bsnl_user_change_datas = bsnl_user_change_cursor.fetchall()
        change = Decimal(0.00)

        if bsnl_user_change_datas is not None:
            for bsnl_user_change_data in bsnl_user_change_datas:
                orderchange = Decimal(0.00)
                if bsnl_user_change_data['unwithdrawn'] is not None :
                    orderchange = Decimal(bsnl_user_change_data['unwithdrawn'])
                    orderRefundId = bsnl_user_change_data['orderRefundId']
                    bsnl_user_orderchange_sql = "select  sum(ifnull(commission_price,0.00)) unallwithdrawn \
                                        from bsnl.order_commission_info\
                                        where 1=1  and user_id ='%s'\
                                        and order_id='%s'" % (userId,orderRefundId)
                    bsnl_user_change_cursor.execute(bsnl_user_orderchange_sql)
                    bsnl_user_orderchange_data = bsnl_user_change_cursor.fetchone()
                    if bsnl_user_orderchange_data is not None and bsnl_user_orderchange_data['unallwithdrawn'] is not None:
                        unallwithdrawn = Decimal(bsnl_user_orderchange_data['unallwithdrawn'])
                        orderchangeabs = orderchange.copy_abs()
                        if orderchangeabs > unallwithdrawn :
                            orderchange = unallwithdrawn * -1
                        change=orderchange+change

        commission = commission + change;
        #全部退款
        bsnl_user_allchange_sql = "SELECT SUM(commission_price) allchange from bsnl.order_commission_info a join commission.%s b on a.order_id = b.order_id" \
                                  " join bsnl.order_info c on b.order_id=c.order_id " \
                                  "  where b.status in (3,4) and c.status in (8) and a.user_id = '%s'" % (last_order_tablename,userId)
        bsnl_user_allchange_cursor = DB.cursor()
        bsnl_user_allchange_cursor.execute(bsnl_user_allchange_sql)
        bsnl_user_allchange_datas = bsnl_user_allchange_cursor.fetchone()

        allchange = Decimal(0.00)
        if bsnl_user_allchange_datas==None or bsnl_user_allchange_datas['allchange'] == None:
            allchange = Decimal(0.00)
        else:
            allchange = Decimal(bsnl_user_allchange_datas['allchange'])
        commission = commission - allchange;
        #累计已提现
        bsnl_user_withdrawn_sql = " select sum(ifnull(amount,0.00)) withdrawn from bsnl.withdraw \
                                where status=1 and user_id = '%s'" % (userId)
        bsnl_user_withdrawn_cursor = DB.cursor()
        bsnl_user_withdrawn_cursor.execute(bsnl_user_withdrawn_sql)
        bsnl_user_withdrawn_datas = bsnl_user_withdrawn_cursor.fetchone()

        withdrawn = Decimal(0.00)
        if bsnl_user_withdrawn_datas==None or bsnl_user_withdrawn_datas['withdrawn'] == None:
            withdrawn = Decimal(0.00)
        else:
            withdrawn = Decimal(bsnl_user_withdrawn_datas['withdrawn'])
        #累计未提现
        bsnl_user_unwithdrawn_sql = " select sum(ifnull(amount,0.00)) unwithdrawn from bsnl.withdraw \
                                where status=2 and user_id = '%s'" % (userId)
        bsnl_user_unwithdrawn_cursor = DB.cursor()
        bsnl_user_unwithdrawn_cursor.execute(bsnl_user_unwithdrawn_sql)
        bsnl_user_unwithdrawn_datas = bsnl_user_unwithdrawn_cursor.fetchone()

        unwithdrawn = Decimal(0.00)
        if bsnl_user_unwithdrawn_datas==None or bsnl_user_unwithdrawn_datas['unwithdrawn'] == None:
            unwithdrawn = Decimal(0.00)
        else:
            unwithdrawn = Decimal(bsnl_user_unwithdrawn_datas['unwithdrawn'])
        #历史未提现
        bsnl_user_hisunwithdrawn_sql = " select ifnull(undrawn_cash,0.00) unwithdrawn from bsnl.old_match_table \
                                where user_id = '%s'" % (userId)
        bsnl_user_hisunwithdrawn_cursor = DB.cursor()
        bsnl_user_hisunwithdrawn_cursor.execute(bsnl_user_hisunwithdrawn_sql)
        bsnl_user_hisunwithdrawn_datas = bsnl_user_hisunwithdrawn_cursor.fetchone()

        hisunwithdrawn = Decimal(0.00)
        if bsnl_user_hisunwithdrawn_datas==None or bsnl_user_hisunwithdrawn_datas['unwithdrawn'] == None:
            hisunwithdrawn = Decimal(0.00)
        else:
            hisunwithdrawn = Decimal(bsnl_user_hisunwithdrawn_datas['unwithdrawn'])
        unDrawn = hisunwithdrawn + unwithdrawn + commission
        totalDrawn = unDrawn + withdrawn
        totalDrawn =  '%.2f' % totalDrawn
        unDrawn =  '%.2f' % unDrawn
        if realName != None:
            realName = realName.replace('\'','')
        #插入佣金表
        commission_sql="insert ignore into `commission`.%s(userid,\
                            realName,\
                            appMobile,\
                            cardNo,\
                            idNo,\
                            role,\
                            bank,\
                            commission,\
                            renStatus,\
                            userStatus,\
                            bankOfDeposit,\
                            companyName,\
                            cardOfUser,\
                            bankMobile,\
                            companyAccount,\
                            lastCommission,\
                            drawn,\
                            unDrawn,\
                            totalDrawn,\
                            isHalfDirector)\
                            values (\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s',\
                            '%s'\
                            )"%(commission_tablename,userId,realName,mobile,card_no,id_number,user_role,bank,commission,ren_status,user_status,bank_of_deposit,company_name,bankRealName,bankmobile,\
                                company_account,selCommission,withdrawn,unDrawn,totalDrawn,halfDirector)
        commission_cursor = DB.cursor()
        # print(commission_sql)
        try :
            commission_cursor.execute(commission_sql)
            DB.commit()
        except(BaseException ):
            print(commission_sql)


if __name__ == '__main__':
    begin = time.time()
    beginTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    # monthOrder()
    # transOrder()
    # createCommissionTable()
    commission()
    total = (time.time() - begin)
    endTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

    m, s = divmod(total, 60)
    h, m = divmod(m, 60)
    print('开始时间：',beginTime)
    print('结束时间：',endTime," 耗时:","%02d 小时 %02d 分钟 %02d 秒" % (h, m, s))
