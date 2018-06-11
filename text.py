# -*- coding: utf-8 -*-
"""
Created on Fri May 25 09:57:37 2018

@author: Lee
"""

import os
import pandas as pd
import numpy as np
import re
import matplotlib.pyplot as plt
from collections import Counter
import jieba
from pylab import mpl
import config


mpl.rcParams['font.sans-serif'] = ['SimHei'] # 指定默认字体
mpl.rcParams['axes.unicode_minus'] = False # 解决保存图像是负号'-'显示为方块的问题

path = 'C:/Users/Lee/Desktop/findajob/lagou_position-master/database/detail/'
file_list = os.listdir(path)
df = pd.read_excel(path+file_list[-5],index_col=7)

#各城市情况
##读入文件
path = 'C:/Users/Lee/Desktop/findajob/lagou_position-master/database/detail/运营/'
file_list = os.listdir(path)
alljob = pd.DataFrame()
for file in file_list:
    df = pd.read_excel(path+file,index_col=7)
    city = re.split('运营_|.xlsx',file)[1]
    df['city'] = city
    alljob = alljob.append(df)
alljob = alljob.dropna()
city = alljob[['city','company']]
job_num = city.groupby('city').count()

##各城市运营岗位数
###公司数
company = alljob[['company','city']].drop_duplicates('company')
company_num = company.groupby(['city']).count()

#各行政区概况
#
def district(df,city):
    district = df[['district','company','city']]
    city_district = district[district['city']==city]
    dis = city_district.groupby('district').count().sort_values('city',ascending=False)
    company = city_district.drop_duplicates('company')
    company_dis = company.groupby('district').count().sort_values('city',ascending=False)
    df2 = pd.merge(dis,company_dis,left_index=True,right_index=True,how='inner')
    df2 = df2[['company_x','company_y']]
    df2.columns = ['岗位数','公司数']
    return df2
#a = district(alljob,'杭州')
#a.to_csv('test.csv',sep = '|')
#district[:8].plot(kind='barh',rot=0,title="北京各行政区运营岗位数量")
#公司情况
#公司行业分布
def ins_count(df):
    df2 = df.drop_duplicates(['company_SN'])
    insdustry = []
    for i in df2['industryField'].dropna():
        i = re.sub('、',',',i)#将顿号替换为逗号
        a = re.split(r',|\s*',i)#去掉符号
        insdustry.extend(a)
    return pd.Series(Counter(insdustry)).sort_values(ascending=False)

def city_ins(df):
    city_list = df['city'].drop_duplicates()
    city_dict = {}
    for city in city_list:
        citydf = df[df['city']==city]
        city_ins = ins_count(citydf)
        #city_ins.name = city
        city_dict[city] = city_ins
    citys = pd.DataFrame(city_dict)
    return citys

#公司数量、招聘数量概况
#company = df['company_SN'].value_counts(sort=True)
#company[:15].plot(kind='barh',rot=0,title="北京各公司招聘运营岗位数量")
#company_num = len(df['company_SN'].drop_duplicates())
def company_job_num_by_city(df):
    city_dict = {}
    city_list = df['city'].drop_duplicates()
    for city in city_list:
        citydf = df[df['city']==city]
        company_rank = citydf['company_SN'].value_counts(sort=True)[:10]
        city_dict[city] =company_rank.index
    city_dict = pd.DataFrame(city_dict)
    return city_dict
company_job_num_by_city(alljob).to_csv('test.csv',sep='|')
#运营分类
#分析主要运营种类
def main_kind(df):
    jieba.load_userdict('./wordlist.txt')
    words = []
    for position in df['position_Name']:
        word = jieba.cut(position)
        for i in word:
            if re.match('[\u4e00-\u9fa5]',i):#去掉非中文字符
                words.append(i)
    positions = Counter(words).most_common(100)#出现在岗位名称里最多的词
    return positions

'''
for position in df['position_Name']:
    if re.search('文案',position):
        print(position)
'''

#df['job_kind'] = df['position_Name'].apply(kind_trans)#细分种类运营数量

#薪资情况
def salary_trans(string):#将薪资范围转换为平均薪资
    if re.search(r'-',string):
        down,up = re.split(r'-',string)
        down = int(re.findall(r'\d+',down)[0])
        up = int(re.findall(r'\d+',up)[0])
        avg = (down+up)/2
    else:
        avg = int(re.findall(r'\d+',string)[0])
    return avg
#df['avg_salary'] = df['salary'].apply(salary_trans)
#avg_salary = df['avg_salary'].mean()#运营平均薪资
#work_year_salary = df[['work_year','avg_salary']].groupby('work_year').describe()#工作年限薪资水平
#com_size_salary = df[['Size','avg_salary']].groupby('Size').describe()#公司规模薪资水平
#com_salary = df[['company_SN','avg_salary']].groupby('company_SN').describe()#公司薪资水平
#kind_salary = df[['job_kind','avg_salary','work_year']].groupby(['job_kind','work_year']).mean().unstack()#不同工种薪资水平
alljob['avg_salary'] = alljob['salary'].apply(salary_trans)
salary = alljob[['avg_salary','city']]
salary_city = salary.groupby('city').mean()
##不同行业薪资情况
def ins_salarys(alljob):
    ins_kind =  ins_count(alljob).index
    ins_salary = {}
    for kind in ins_kind:
        ins_df = alljob[alljob['industryField'].str.contains(kind)]
        avg_salary = ins_df['avg_salary'].mean()
        ins_salary[kind] = {
                'avg_salary':avg_salary,
                'company_num':len(ins_df.drop_duplicates('company_SN')),
                'job_num':len(ins_df)
                }
    ins_salary = pd.DataFrame(ins_salary).T.sort_values('avg_salary',ascending=False)
    ins_salary['avg_salary'] = ins_salary['avg_salary'].round(decimals=2)
    ins_salary.to_csv('test.csv',sep='|')
##不同工作年限薪资情况
def work_year_salary(alljob):
    citydf = alljob[['work_year','avg_salary','city']]
    wy_salary = citydf.groupby(['work_year','city']).mean().unstack().round(decimals=2)
    wy_salary.to_csv('test.csv',sep='|')
##不同运营工种薪资情况
def kind_trans(title):#用以上的对应关系细分运营种类
    kinds = config.operating_category
    for kind in kinds:
        if re.search(kind,title):
            return kinds[kind]    

def job_kind_salary(alljob):
    alljob['job_kind'] = alljob['position_Name'].apply(kind_trans)
    job_kind_num = alljob.groupby('job_kind').count()
    job_kind_salary = alljob[['avg_salary','job_kind','work_year']]
    job_kind_salary = job_kind_salary.groupby(['job_kind','work_year']).mean()
    job_kind_salary.round(decimals=2).sort_values('avg_salary',ascending=False).unstack().to_csv('test.csv',sep='|')
    return job_kind_salary.round(decimals=2).sort_values('avg_salary',ascending=False).unstack()

#各城市不同的运营细分岗位数量
def count_kind(df):
    df['job_kind'] = df['position_Name'].apply(kind_trans)
    catejob = df[df['job_kind'].notnull()]
    catejob = catejob[['city','job_kind','avg_salary']]
    cate = catejob.groupby(['city','job_kind']).count().unstack()
    cate.to_csv('test.csv',sep='|')

##不同公司规模的薪资差异
def company_size_salary(df):
    df = df[['Size','avg_salary','work_year']]
    company_size_salary = df.groupby(['Size','work_year']).mean()
    return company_size_salary.sort_values('avg_salary',ascending=False)
    
##特定技能带来的薪资变化
def skill_salary(df):
    #skills = ['抗压能力','勤奋','区块链','谈判','应变能力','驱动力','吃苦耐劳','加班','数据挖掘','二次元','开朗','进取心','英语','女性','建模','英语六级','德语','美女','Python']
    skills = fre_word(df)
    skllls_dict = {}
    for skill in skills.index:
        skill_df = df[df['position_detail'].str.contains(skill)]
        skill_salary = skill_df['avg_salary'].mean() - df['avg_salary'].mean()
        skllls_dict[skill] = skill_salary
    return pd.Series(skllls_dict).sort_values(ascending=False)
        
#遍历所有职位的描述，寻找最符合设定的岗位
def fre_word(df):#分析JD中高频词
    words = []
    jieba.load_userdict('./wordlist.txt')
    blockword = config.word_filter#去掉一些无意义的词
    for jd in df['position_detail']:
        word = jieba.cut(jd,cut_all=False)
        for i in word:
            if re.match('[\u4e00-\u9fa5]',i):#去掉非中文字符
                if not i in blockword:
                    words.append(i)
    #jds = pd.Series(Counter(words)).sort_values(ascending=False)[:1000]#出现在岗位名称里最多的词
    fre_word = pd.Series(Counter(words)).sort_values(ascending=False)
    fre_word = fre_word[fre_word>500]
    return fre_word
#all_word = fre_word(alljob)
#get tag
def get_tag(df):
    taglist = []
    for tag in df['job_tags']:
        tags = re.split(',',tag)
        taglist.extend(tags)
    count = pd.Series(Counter(taglist)).sort_values(ascending=False)
    return count

def tag_salary(df):
    tags = get_tag(df)
    tag_dict = {}
    line = 0 
    avg_salary = df[df['work_year']=='1-3年']['avg_salary'].mean()
    for tag in tags.index:
        if re.search('\+',tag):
            continue
        tag_df = df[df['job_tags'].str.contains(tag)]
        tag_salary = tag_df['avg_salary'].mean() - avg_salary
        tag_dict[tag] = {
                'tag_salary':tag_salary,
                'tag_num':tags[line]
                }
        line += 1
    data = pd.DataFrame(tag_dict).T
    data = data[data['tag_num']>300]
    return data

#不同工种高频词比较
def job_kinds_freword(df):
    kinds = df['job_kind'].drop_duplicates()
    kinds = kinds[kinds.notnull()]
    results = {}  
    for kind in kinds:
        detail = df[df['job_kind']==kind]
        jds = fre_word(detail)
        results[kind] = jds
    result = pd.DataFrame(results)
    result_num = pd.DataFrame(results).dropna()#各工种中最高频的词
    result_percent = (result/result.sum()).dropna()#各工种中最高频的词占比
    result_sub = pd.concat([result_percent,result_percent.mean(axis = 1)],axis=1)
    sub = result_percent.sub(result_percent.mean(axis=1),axis=0)#运营各工种的特征词
    return sub
#sub_word = job_kinds_freword(alljob)

#对JD匹配度进行评分
def get_score(string):
    white_list = config.white_list
    black_list = config.black_list
    score = 0
    for word in white_list:
        if re.search(word,string,re.IGNORECASE):
            score += white_list[word]
    for word in black_list:
        if re.search(word,string,re.IGNORECASE):
            score -= black_list[word]
    return score
#df['jd_score'] = df['position_detail'].apply(get_score)
#df_score = df[df['jd_score'] >5]
#top_100 = df.sort_values('jd_score',ascending=False)[:1000][df['work_year']=='1-3年']
#筛选出近期增加的JD
df1 = pd.read_excel(r'C:\Users\Lee\Desktop\findajob\lagou_position-master\database\detail\运营\运营_北京.xlsx')
df2 = pd.read_excel(r'C:\Users\Lee\Desktop\findajob\lagou_position-master\database\运营_北京_6-7.xlsx').dropna()
df2 = df2.append(df1)
df2 = df2.append(df1)
df2 = df2.drop_duplicates('position_id',keep=False)#得到新增的岗位
df2 = df2.dropna(axis=1, how='all')
df2= pd.DataFrame(df2)
detail = pd.read_excel(r'add_6.7.xlsx')
df3 = pd.merge(df2,detail,on ='position_id')
df3['job_score'] = df3['position_detail'].apply(get_score)
df3 = df3[df3['hr_ratio']=='100%']
df3 = df3[df3['work_year']=='1-3年']
df3['avg_salary']= df3['salary'].apply(salary_trans)
df3 = df3[df3['avg_salary']>=10]
