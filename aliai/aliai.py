#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys,os
import base64
import time
import json

# from urlparse import urlparse
from com.aliyun.api.gateway.sdk import client
from com.aliyun.api.gateway.sdk.http import request
from com.aliyun.api.gateway.sdk.common import constant
import traceback
import urllib.request as urllib
import base64

def get_img_base64(img_file):
    with open(img_file, 'rb') as infile:
        s = infile.read()
        # return base64.b64encode(s)
        return str(base64.b64encode(s), encoding='utf-8');


def predict(url, appcode, img_base64, kv_config, old_format):
    if not old_format:
        param = {}
        param['image'] = img_base64
        if kv_config is not None:
            param['configure'] = json.dumps(kv_config)
        body = json.dumps(param)
    else:
        param = {}
        pic = {}
        pic['dataType'] = 50
        pic['dataValue'] = img_base64
        param['image'] = pic

        if kv_config is not None:
            conf = {}
            conf['dataType'] = 50
            conf['dataValue'] = json.dumps(kv_config)
            param['configure'] = conf


        inputs = { "inputs" : [param]}
        body = json.dumps(inputs)


    headers = {'Authorization' : 'APPCODE %s' % appcode}
    request = urllib.Request(url = url, headers = headers, data = bytes(body, encoding='utf-8'))
    try:
        response = urllib.urlopen(request, timeout = 10)
        return response.code, response.headers, response.read()
    except urllib.HTTPError as e:
        return e.code, e.headers, e.read()


def demo(imgFile):
    appcode = '23025dbe9dcd4d74a0d19577460ebf93'
    url = 'https://form.market.alicloudapi.com/api/predict/ocr_table_parse'
    img_file = imgFile
    #如果输入带有inputs, 设置为True，否则设为False
    is_old_format = False
    #  html/xlsx(excel)/json
    config = {'format': 'html', 'finance': False, 'dir_assure': False}
    #如果没有configure字段，config设为None
    #config = None

    img_base64data = get_img_base64(img_file)
    stat, header, content = predict( url, appcode, img_base64data, config, is_old_format)
    if stat != 200:
        print('Http status code: ', stat)
        print('Error msg in header: ', header['x-ca-error-message'] if 'x-ca-error-message' in header else '')
        print('Error msg in body: ', content)
        exit()
    if is_old_format:
        result_str = json.loads(content)['outputs'][0]['outputValue']['dataValue']
    else:
        result_str = content.decode()
    print(result_str);
    result = json.loads(result_str)
    tables = result['tables']
    print(tables)
    return tables

# if __name__ == '__main__':
#     demo()