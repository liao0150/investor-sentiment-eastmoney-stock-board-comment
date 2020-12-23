#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 27 12:06:28 2019

@author: liaosimin
"""

import os
import pandas as pd


def DataProcessing(filename):
    # load data
    names = ['read_ct','comment_ct','post_link','title','source_bar','publish_date1']
    dtype = {'publish_date1': str, 'post_link': str, 'title': str}
    df = pd.read_csv(filename,sep='\t',encoding='utf-8', engine='python',
                 names=names,dtype=dtype,error_bad_lines=False)
    df = df.drop(columns=['source_bar'])
    df = df.dropna()
    df = df[~df['post_link'].isin([""])]
    df = df[~df['publish_date1'].isin(["","na","Nan","NaN","None"])]
    df = df.drop_duplicates('post_link',keep='last')
    df = df.reset_index()

    # extract month
    publish_month = []
    for i in range(len(df['post_link'])):
        publish_month.append(str(df['publish_date1'][i])[0:2])

    df['publish_month'] = pd.to_numeric(publish_month)
    df['seq'] = df['post_link'].index.values

    breakpoint = df[df['publish_month'] == 12].index.values
    try:
        year = 2018
        breakpoints = []
        breakpoints.append({'point':breakpoint[0],'year':year})
        for i in range(1,len(breakpoint)):
            if breakpoint[i-1] == breakpoint[i] - 1:
                pass
            else:
                year += -1
                breakpoints.append({'point':breakpoint[i],'year':year})

        # calculate year
        publish_year = ['2019' for _ in range(len(df['post_link']))]

        for i in range(len(df['post_link'])):
            for j in range(len(breakpoints)):
                if df['seq'][i] >= breakpoints[j]['point']:
                    publish_year[i] = breakpoints[j]['year']

        df['publish_year'] = publish_year
    except:
        publish_year = ['2019' for _ in range(len(df['post_link']))]
        df['publish_year'] = publish_year


    publish_date = []
    for i in range(len(df['post_link'])):
        a = str(df['publish_year'][i])+'-'+str(df['publish_date1'][i])
        publish_date.append(a)
    df['publish_date'] = publish_date

    # output processed data
    df = df.loc[:,['seq','publish_date','read_ct','comment_ct','post_link','title','publish_date1']]
    df.to_csv(filename,index=False,sep='\t',encoding='utf-8')

c_filenames = os.listdir()
for file in c_filenames:
    try:
        DataProcessing(filename=file)
        print(file, 'success')
    except:
        print(file,'error')
