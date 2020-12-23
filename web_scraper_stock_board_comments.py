#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 12 17:15:28 2019

@author: liaosimin
"""

import requests
import time
from pyquery import PyQuery as pq
import re
import random
import pandas as pd
import _thread
import numpy as np


def get_one_page(url):
    """
    Get all posts in the page.
    
    """
    try:
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/65.0.3325.181 Safari/537.36',
                   'Connection': 'close'}

        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            return response.text
        return None
    except requests.RequestException as e:
        with open('exception_log.txt', 'a', encoding='utf-8') as file:
            file.write('\t'.join([str(url),str(e),time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())]))
            file.write('\n')


def get_year(html):
    """
    Get year data from the page, month and day can be extracted from each post.
    
    """
    try:
        doc = pq(html)
        publish_info = doc('div #zwconttb .zwfbtime').text()
        publish_info = re.match(".*?((\d{4})-\d{2}-\d{2}).*", publish_info)
        if publish_info:
            publish_year = publish_info.group(2)
        else:
            publish_year = ""
    except:
        publish_year = ""
    return publish_year


def parse_one_page1(html):
    """
    Parse and get text of each post in the page.
    
    """
    try:
        doc = pq(html)
        results = doc('div #articlelistnew div')
        results1 = doc('div #articlelistnew div .l3')
        link1 = results1.children('a').attr('href')
        link1 = 'http://guba.eastmoney.com' + link1
        html1 = get_one_page(link1)
        year = get_year(html1)
        for result in results.items():
            post_link = result.find('.l3 a').attr('href')
            if post_link:
                f_time = result.find('.l5').text()
                f_time = re.match(".*?(\d{2}-\d{2}).*", f_time)
                if f_time:
                    ft = str(year) + '-' + f_time.group(1)
                    yield [result.find('.l1').text(),
                           result.find('.l2').text(),
                           result.find('.l3 a').attr('href'),
                           result.find('.l3 a').attr('title'),
                           ft]
        print(year)
    except:
        pass


def write_to_file(page, code):
    """
    Write to txt file for later data processing.
    
    """
    url = 'http://guba.eastmoney.com/list,'+str(code)+',f_'+str(page)+'.html'
    html = get_one_page(url)
    if html:
        for item in parse_one_page1(html):
            f = 'post_' + str(code) + '.txt'
            with open(f, 'a', encoding='utf-8') as file:
                file.write('\t'.join([str(member) for member in item]))
                file.write('\n')


def main(pagenum1,pagenum2,code,sleepsec):
    # keep track of progress
    print(code, 'start')

    with open('start2_log.txt', 'a', encoding='utf-8') as file:
        file.write('\t'.join([code, 'start', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())]))
        file.write('\n')

    for i in range(pagenum1,pagenum2):
        page = i + 1
        write_to_file(page=page, code=code)
        print(page, 'success')
        time.sleep(sleepsec)
    # keep track of progress
    print(code, 'success')

    with open('scrap2_log.txt', 'a', encoding='utf-8') as file:
        file.write('\t'.join([code, 'success', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())]))
        file.write('\n')


# Load stock list from zhongzheng500 index
df = pd.read_csv('zhongzheng500_conlist_buchong.csv', skiprows=1, names=['code', 'page','codenum'],
                 dtype={'code': str, 'page': np.int,'codenum': np.int})


# Multi-thread
for code in df['code']:
    _thread.start_new_thread(main, (0, 750, code, random.random()*1))

time.sleep(3600*4)

