import pandas as pd
import datetime
from datetime import timedelta, datetime
from copy import deepcopy
from bson import ObjectId
from pymongo import MongoClient
import sys

import os
import json
import pprint

from general_helper import *

corp_weeks = json.load(open('corp_weeks.json'))

# Prod TRA DB
client = MongoClient(tradb_conn_str)
db = client['TraDB']

try:
    # first find the campaign id 
    campaign = db.campaigns.find_one({'program': 'AFFILIATE',
                                    'participant': 'CITADEL MARKETING GROUP'},
                                    {'premiums':0})
    if campaign != None:
        citadel_campaignid = campaign["_id"]
        
        date_range = create_date_range(1)
        st_date = date_range[0]
        en_date = date_range[1]

        print(str(st_date.strftime("%Y-%m-%d")))

        # finding leads for last week
        leads = list(db.leads.find({'dateCreated': {'$gte': st_date, '$lt': en_date},
                                    'campaign': citadel_campaignid}))
        if leads != None:
            if len(leads) > 0:
                print("Total Number of Leads found",len(leads))

                unique_tagId_list=[]
                marketingSpendJson_bulk=[]
                for lead in leads:
                    get_tag=False
                    for contact in lead.get('contactAttempts', []):
                        if contact.get('tagId', '') != '':
                            if check_on_date_and_tagid(st_date.strftime("%Y-%m-%d"),
                                                    en_date.strftime("%Y-%m-%d"),
                                                    str(contact.get('tagId')),"CITADEL MARKETING GROUP") == True:

                                if get_tag == False:
                                    if contact["tagId"] in unique_tagId_list:
                                        for lcj in marketingSpendJson_bulk:
                                            if lcj["contactAttempts"][0]["tags"] == contact["tagId"]:
                                                lcj["leads"] = lcj["leads"] + 1     

                                    else:
                                        DateCreated=contact.get('dateCreated').strftime("%Y-%m-%d")
                                        tags_container=[]
                                        tag={}
                                        tag['tags']=contact["tagId"]
                                        tags_container.append(tag)

                                        marketingSpendJson={}
                                        marketingSpendJson["campaign"]=lead["campaign"]
                                        marketingSpendJson["LeadCorpWeek"]=corp_weeks.get(str(DateCreated))
                                        marketingSpendJson["dateCreated"]=pd.to_datetime(DateCreated)
                                        marketingSpendJson["contactAttempts"]=tags_container
                                        marketingSpendJson["spent"]=0
                                        marketingSpendJson["subtype"]="marketing_spend"
                                        marketingSpendJson["type"]="spend"
                                        marketingSpendJson["leads"]=1

                                        marketingSpendJson_bulk.append(marketingSpendJson)
                                        unique_tagId_list.append(contact.get('tagId'))
                                get_tag= True

                print("Unique tagIds",len(unique_tagId_list))

                if len(unique_tagId_list) > 0:

                    num_of_lead= len(leads)
                    charge = num_of_lead * .75
                    num_of_tags = len(list(set(unique_tagId_list)))
                    per_tag_spent = charge / num_of_tags
                    print("leads spent per unique tags ", per_tag_spent)

                    for sp in marketingSpendJson_bulk:
                        sp["spent"] = per_tag_spent
                    pprint.pprint(marketingSpendJson_bulk) 

                else:
                    print("Leads already exist")


                if len(marketingSpendJson_bulk) > 0:
                    mongodb_many_to_many_insert(adsSpentCollection,marketingSpendJson_bulk)
                    for v in marketingSpendJson_bulk:
                        mongoid = str(v['_id'])
                        del v['_id']
                        v['mongo_id'] = mongoid
                        v['campaign'] = getTradbCampaignInfo(campaignsCollection,str(v['campaign']))
                        v['dateCreated'] = v['dateCreated'].isoformat()
                        for contA in v['contactAttempts']:
                            contA['tags']=getTradbTagsInfo(tagsCollection,str(contA['tags']))

                    adSpentElasticIndexing(es, marketingSpendJson_bulk, FbAdSpentIndex, FbAdSpentDoctype)
            else:
                print("Leads count for Citadel = 0"+" "+ "for Date",st_date)

except Exception as e:
    try:
        myteamsalert("Alert ! Please Check Something Seems Wrong With Citadel Spend Service"+" "+ "Exception : "+str(e))
    except:
        print("Error While Sending Teams Alert")
    print("Error :", e)