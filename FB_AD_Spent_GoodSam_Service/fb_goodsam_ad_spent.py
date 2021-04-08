# Contributors: Muhammad Tahir and Shah Fahad, Hassan Mehmood
# Date last Modified: 25. March, 2021
# Descrition: This notebook will insert Good Sam C2C FB Costs to Warehouse.

import pandas as pd
from datetime import datetime
import time
import requests
from requests import get,post
import sys
import os
import json
import petl as etl
import pprint

from general_helper import *

# Attaching Corp Week Id using Dim_CorpWeek table in warehouse
dw_conn = get_dw_conn(server,database,username,password)
# Converting datetime object to Corp Week Id
corp_weeks = json.load(open('corp_weeks.json'))

ftp = get_ftp_connection(ftp_path,ftp_username,ftp_password)
ftp.cwd('/fbreports/goodsam')
files_on_ftp = ftp.nlst()
# Cleaning existing file lists
try:
    files_on_ftp.remove('.')
    files_on_ftp.remove('..')
except:
    pass

PARTICIPANT = 'Good Sam'
REQUIRED_COLUMNS = [['Campaign Name', 'CampaignId', 'adsetid', 'adid', 'Day', 'AdSetName', 'AdName', 
                     'social_reach', 'impressions', 'frequency', 'Result Type', 'Results', 'Cost per Result', 
                     'spent', 'cpc', 'page_likes', 'post_engagement', 'video_views', 'cost_per_lead_lp', 
                     'leads_form', 'clicks','dim_dateid', 'CorpWeekId', 'dim_participantid', 'dimProgramId']]
try:
    # for filename in files_on_ftp:
        
    #     if check_existing_files(PARTICIPANT,filename) == False:
    #         # Old file
    #         continue
        
    #     print('Downloading', filename)
    # #     ftp.retrbinary('RETR ' + filename, open(filename, 'wb').write)
    #     time.sleep(1)
    #     fb_costs = pd.read_excel(filename, dtype={'Campaign ID':str,'Ad Set ID':str, 'Ad ID': str}).fillna('')
    file=("TRA-Monday-Report-Mar-15-2021-Mar-21-2021.xlsx")
    fb_costs = pd.read_excel(file)
    print(len(fb_costs), 'Number of Rows in file.')

    # if len(fb_costs) == 0:
    #     print('0 Rows found in file, Continue to next file.')
    #     continue

    fb_costs = fb_costs.rename(columns={
        'Campaign ID': 'CampaignId', 'Ad Set ID': 'adsetid', 'Ad set ID': 'adsetid',
        'Ad ID': 'adid', 'Ad Set Name': 'AdSetName', 'Ad set name': 'AdSetName',
        'Ad Name': 'AdName', 'Ad name': 'AdName', 'Reach': 'social_reach',
        'Impressions': 'impressions','Frequency': 'frequency',
        'Amount Spent (USD)': 'spent', 'Amount spent (USD)': 'spent',
        'CPC (All)': 'cpc', 'CPC (all)': 'cpc','Page Likes': 'page_likes', 
        'Post Engagement': 'post_engagement', 'Post engagement': 'post_engagement', 'Video Plays': 'video_views',
        'Video plays': 'video_views', 'Cost per Lead': 'cost_per_lead_lp', 'Page likes': 'page_likes', 
        'Leads (Form)': 'leads_form', 'Leads (form)': 'leads_form',
        'Clicks (All)': 'clicks', 'Clicks (all)': 'clicks', 
        'Campaign Name': 'CampaignName','Campaign name': 'CampaignName'})

    # Converting date string into datetime object
    fb_costs['Day'] = pd.to_datetime(fb_costs['Day'])
    # Converting datetime object to Corp Week Id
    fb_costs['CorpWeek'] = fb_costs[['Day']] \
        .apply(lambda x: corp_weeks.get(str(x[0])[:10], 0), axis=1)
    # Attaching Corp Week Id using Dim_CorpWeek table in warehouse

    Dim_CorpWeek = pd.read_sql("""
    SELECT * FROM Dim_CorpWeek""", dw_conn) 
    Dim_Date = pd.read_sql("""
    SELECT * from Dim_Date""", dw_conn)

    Dim_CorpWeek.CorpWeek = Dim_CorpWeek.CorpWeek.apply(int)
    fb_costs = pd.merge(left=fb_costs, right=Dim_CorpWeek, how='left', on='CorpWeek')
    fb_costs['date'] = fb_costs[['Day']].apply(lambda x: str(x[0])[:10], axis=1)
    Dim_Date.date = Dim_Date.date.apply(str)
    fb_costs = pd.merge(left=fb_costs, right=Dim_Date, on='date', how='left')
    fb_costs = fb_costs.rename(columns={'DateId': 'dim_dateid','Day': 'DateCreated'})

    REQUIRED_COLUMNS = ['CampaignName', 'CampaignId', 'adsetid', 'adid', 'AdSetName', 'AdName',
    'social_reach', 'impressions', 'frequency', 'spent',
    'clicks', 'CorpWeekId', 'dim_dateid','CorpWeek', 'DateCreated']

    fb_costs = fb_costs[REQUIRED_COLUMNS]

    fb_costs['dim_participantid'] = 7430
    fb_costs['dimProgramId'] = 1000000195
    fb_costs['CampaignManagerId'] = fb_costs[['AdName']].apply(lambda x: get_campaign_manager_id_from_adname(x[0]), axis=1)

    fb_costs['resort'] = ''
    fb_costs['ad_account_id'] = 0
    fb_costs['leads'] = 0
    fb_costs['post_engagement'] = 0

    for i in ['CampaignName', 'adsetid', 'adid', 'AdSetName', 'AdName', 'resort']:
        fb_costs[i] = fb_costs[i].fillna('').apply(str)

    for i in ['social_reach', 'impressions', 'CampaignManagerId', 'post_engagement',
            'clicks', 'CorpWeekId', 'dim_dateid', 'dim_participantid', 'dimProgramId',
            'ad_account_id','CorpWeek','leads']:
        fb_costs[i] = fb_costs[i].fillna(0).replace('', 0).apply(int)

    for i in ['frequency', 'spent','post_engagement']:
        fb_costs[i] = fb_costs[i].fillna(0.0).replace('', 0.0).apply(float)

    # decoding ascii's
    fb_costs['AdSetName'] = fb_costs[['AdSetName']] \
        .apply(lambda x: encode_agent_name(x[0]).title(), axis=1)
        
    payloadBulk = generate_goodsam_ads_spent(fb_costs)
    # Insert in MongoDb and Elasticsearch
    mongodb_many_to_many_insert(adsSpentCollection,payloadBulk)

    elasticBulk=[]
    for v in payloadBulk:
        mongoid = str(v['_id'])
        del v['_id']
        v['mongo_id'] = mongoid
        v['campaign'] = getTradbCampaignInfo(campaignsCollection,str(v['campaign']))
        v['dateCreated'] = v['dateCreated'].isoformat()
        for contA in v['contactAttempts']:
            contA['tags']=getTradbTagsInfo(tagsCollection,str(contA['tags']))
        elasticBulk.append(json.dumps(v))
        pprint(payloadBulk)

    adSpentElasticIndexing(es, payloadBulk, FbAdSpentIndex, FbAdSpentDoctype)

    dw_conn.close()
    ftp.quit()
except Exception as e:
    print(e)