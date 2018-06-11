# -*- coding: utf-8 -*-
"""
Created on Tue Jan 23 09:12:37 2018
@author: inews
爬取拉勾网职位名称为“python数据分析”的职位信息
"""

import requests
import pandas as pd
import numpy as np
import time
import random
from bs4 import BeautifulSoup
from time import sleep
import re
import datetime
import os
import warnings
import datetime 

proxy = {'http':'58.249.35.42:8118'}   #代理IP，如果失效则需要更换

USER_AGENT_LIST = [
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
        "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
        ]

def get_cookies():
    """return the cookies after your first visit"""  #自动获取cookie，参考自EclipseXuLu/Lagou/Jobspider/m_lagou_spider.py
    headers = {
        'User-Agent':np.random.choice(USER_AGENT_LIST),
        'Host':'www.lagou.com',
        'X-Anit-Forge-Code':'0',
        'X-Anit-Forge-Token':'None',
        'X-Requested-With':'XMLHttpRequest'
        }
    url = 'https://www.lagou.com/'
    response = requests.get(url, headers=headers, timeout=10)
    return response.cookies

def get_soup(url):
    cookies = get_cookies()
    headers = {
            'User-Agent':np.random.choice(USER_AGENT_LIST),
            'Host':'www.lagou.com',
            'Connection':'keep-alive',
            'Origin':'https://www.lagou.com',
            'Referer':'https://www.lagou.com/',
            'X-Anit-Forge-Code':'0',
            'X-Requested-With':'XMLHttpRequest'
            }
    result = requests.get(url,headers = headers,cookies=cookies,proxies = proxy)
    soup = BeautifulSoup(result.content, 'html.parser')
    return soup

def position_detail(position_id):
    warnings.filterwarnings("ignore")
    cookies = get_cookies()
    url = 'https://www.lagou.com/jobs/%s.html' %position_id
    headers = {
            'User-Agent':np.random.choice(USER_AGENT_LIST),
            'Host':'www.lagou.com',
            'Connection':'keep-alive',
            'Origin':'https://www.lagou.com',
            'Referer':url,
            'X-Anit-Forge-Code':'0',
            'X-Requested-With':'XMLHttpRequest'
            }
    result = requests.get(url,headers = headers,cookies=cookies,proxies = proxy)
    soup = BeautifulSoup(result.content, 'html.parser')
    #-----
    position_req = soup.find('dd',class_="job_bt")
    job_advantage = soup.find('dd',class_="job-advantage")
    work_address = soup.find('dd',class_="job-address clearfix").div
    for address in work_address:
        if address.name is None and re.search('[\u4e00-\u9fa5]',address):
            job_address =  re.split('-|\s*',address)[3]
            break
    hr_responce = soup.find('div',class_="publisher_data").find_all('div')[0].find('span',class_="tip").i.text
    hr_ratio = soup.find('div',class_="publisher_data").find_all('div')[1].find('span',class_="tip").i.text
    job_tags = soup.find('ul',class_="position-label clearfix").find_all('li')
    tags = []
    for tag in job_tags:
        tags.append(tag.text)
    tags = ','.join(tags)
    details = {
            'position_id':position_id,
            'position_detail':position_req.text,
            'job_advantage':job_advantage.text,
            'work_address':job_address,
            'hr_responce':hr_responce,
            'hr_ratio':hr_ratio,
            'job_tags':tags
            }
    #----
    sleep(random.uniform(2,3))
    if position_req is None:     #有的页面返回空值，需先进行判断，不然position_req.text会报错
        return
    return details

def get_position(keyword,city,form_data):
    url = 'https://www.lagou.com/jobs/positionAjax.json?city=%s&needAddtionalResult=false&isSchoolJob=0' % city 
    headers = {
            'User-Agent':np.random.choice(USER_AGENT_LIST),
            'Host':'www.lagou.com',
            'Connection':'keep-alive',
            'Origin':'https://www.lagou.com',
            'Referer':'https://www.lagou.com/jobs/list_python%E6%95%B0%E6%8D%AE%E5%88%86%E6%9E%90?oquery=python%E7%88%AC%E8%99%AB&fromSearch=true&labelWords=relative&city=%E6%B7%B1%E5%9C%B3',
            'X-Anit-Forge-Code':'0',
            'X-Anit-Forge-Token':'None',
            'X-Requested-With':'XMLHttpRequest'
            }
    #for x in range(i,i+5):      #总计有30页，但一次爬取超过7-8页就会返回错误，所以在这设置一次爬取5页内容
    try:
        cookies = get_cookies()
        result = requests.post(url,headers = headers,cookies=cookies,proxies = proxy,data = form_data)
        resultdic = result.json()
        jobsInfo = resultdic['content']['positionResult']['result']
        jobsInfo_list = []
        for position in jobsInfo:
            position_dict = {
                'position_Name':position['positionName'],
                'work_year':position['workYear'],
                'salary':position['salary'],
                'company':position['companyFullName'],
                'company_SN':position['companyShortName'],
                'Size':position['companySize'],
                'education':position['education'],
                'district':position['district'],
                'industryField':position['industryField'],
                'position_id' : position['positionId']
                }
            #position_id = position['positionId']   
            #position_dict['position_detail'] = position_detail(position_id)
            jobsInfo_list.append(position_dict)
        return jobsInfo_list
    except:
        pass 

def form_maker(page_num,keyword,dis=None,hy=None,jd=None,zone=None):
    form_data = {
                'first' : 'true',
                'pn' : page_num,
                'kd' : keyword
                }
    if not dis is None:
        form_data['district'] = dis
    if not hy is None:
        form_data['hy'] = hy
    if not jd is None:
        form_data['jd'] = jd
    if not zone is None:
        form_data['businessZone'] = zone
    return form_data

def get_filter(word,keyword,city):
    url = 'https://www.lagou.com/jobs/list_%s?px=default&city=%s#filterBox' % (keyword,city)
    lists = []
    if word == 'district':
        soup = get_soup(url)
        for a in soup.find('div',class_="contents").find_all('a'):
            if not re.match(r'不限',a.string):
                lists.append(a.string)
    elif word == 'bizArea':
        zones = get_filter('district',keyword,city)
        for zone in zones:
            url = 'https://www.lagou.com/jobs/list_%s?px=default&city=%s&district=%s#filterBox' % (keyword,city,zone)
            soup = get_soup(url)       
            for a in soup.find('div',class_="detail-items").find_all('a'):
                if not re.match(r'不限',a.string):
                    lists.append(a.string)     
            sleep(np.random.uniform(3,5))
    elif word =='hy':
        lists = ['移动互联网','电子商务','金融','企业服务','教育','文化娱乐','游戏','O2O','硬件','社交网络','旅游','医疗健康','生活服务','信息安全','数据服务','广告营销','分类信息','招聘','其他']
    elif word == 'jd':
        lists = ['未融资','天使轮','A轮','B轮','C轮','D轮及以上','上市公司','不需要融资']
    elif word == 'gj':
        lists = ['3年及以下','3-5年','5-10年','10年以上','不要求']
    else:
        url = 'https://www.lagou.com/jobs/list_%s?px=default&city=%s&district=%s#filterBox' % (keyword,city,word)
        soup = get_soup(url)   
        if soup is None:
            return
        for a in soup.find('div',class_="detail-items").find_all('a'):
            if not re.match(r'不限',a.string):
                lists.append(a.string)     
    return lists
    
def job_num(url):
    soup = get_soup(url)
    try:
        num = soup.find('a',id="tab_pos").span.string
    except:
        return 0
    sleep(random.uniform(1,3))
    return num

def fill_detail(df):
    details = []
    line = 0
    for position_id in df['position_id']:
        try:
            detail = position_detail(position_id)
            details.append(detail)
        except Exception as e:
            print(e)
            pass
        line += 1
        rate = line/len(df)
        print('%.2f%%' % (rate * 100))
    df = pd.DataFrame(details)
    return df
            
def crawler(keyword,city):
    url = 'https://www.lagou.com/jobs/list_%s?px=default&city=%s#filterBox' % (keyword,city)
    num = job_num(url)
    fullurl = []
    form_list = [] #储存数据
    if num == '500+':
        diss = get_filter('district',keyword,city)
        for dis in diss:
            print(dis)
            url = 'https://www.lagou.com/jobs/list_%s?px=default&city=%s&district=%s#filterBox' % (keyword,city,dis)
            num = job_num(url)
            if num == '500+':
                hys = get_filter('hy',keyword,city)
                for hy in hys:
                    url = 'https://www.lagou.com/jobs/list_%s?px=default&city=%s&district=%s&hy=%s#filterBox' % (keyword,city,dis,hy)
                    num = job_num(url)
                    if num == '500+':
                        jds = get_filter('jd',keyword,city)
                        for jd in jds:
                            url = 'https://www.lagou.com/jobs/list_%s?px=default&city=%s&district=%s&hy=%s&jd=%s#filterBox' % (keyword,city,dis,hy,jd)
                            num = job_num(url)
                            if num == '500+':
                                zones = get_filter(dis,keyword,city)
                                for zone in zones:
                                    url = 'https://www.lagou.com/jobs/list_%s?px=default&city=%s&district=%s&hy=%s&jd=%s&bizArea=%s#filterBox' % (keyword,city,dis,hy,jd,zone)
                                    num = job_num(url)
                                    if num == '500+':
                                        fullurl.append(url)
                                        for x in range(1,31):
                                            form_data = form_maker(x,keyword,dis,hy,jd,zone)
                                            form_list.append(form_data) 
                                    else:
                                        time = (int(num)//15) + 1
                                        for x in range(1,time+1):
                                            form_data = form_maker(x,keyword,dis,hy,jd,zone)
                                            form_list.append(form_data) 
                            else:
                                time = (int(num)//15) + 1
                                for x in range(1,time+1):
                                    form_data = form_maker(x,keyword,dis,hy,jd)
                                    form_list.append(form_data)
                    else:
                        time = (int(num)//15) + 1
                        for x in range(1,time+1):
                            form_data = form_maker(x,keyword,dis,hy)
                            form_list.append(form_data)
            else:
                time = (int(num)//15) + 1
                for x in range(1,time+1):
                    form_data = form_maker(x,keyword,dis)
                    form_list.append(form_data)
    else:
        time = (int(num)//15) + 1
        for x in range(1,time+1,5):
            form_data = form_maker(x,keyword)
            form_list.append(form_data)
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')#现在
    with open('./fullurl.txt','a') as f:
        f.write('\n')
        f.write(nowTime)
        f.write('\n')
        f.write(str(fullurl))
    return form_list
           
def worker(keyword,city):
    df = pd.DataFrame()
    print(keyword,city,'正在抓取顺序')
    start =time.clock()
    formlist = crawler(keyword,city)
    line = 0
    for form_data in formlist:
        data = get_position(keyword,city,form_data)
        df1 = pd.DataFrame(data)
        df = df.append(df1)
        sleep(np.random.uniform(2,5))
        line += 1
        rate = line/len(formlist)
        print('%.2f%%' % (rate * 100))
    df.drop_duplicates(['position_id']).to_excel('./database/'+keyword+'_'+city+'.xlsx')
    end =time.clock()
    print('Running time: %s Seconds'%(end-start))
        

