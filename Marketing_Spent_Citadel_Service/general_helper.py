import petl as etl
import pandas as pd
from datetime import datetime
from requests import get,post
from dateutil import parser
import json
import logging
from datetime import timedelta
import dateutil.relativedelta
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from load_env_var import *
from connection import *

# Connection to Mongo Tradb live and Dev
tradbDev_conn = mongodb_connection(tradbDev_str,tradbDev_db)
tradbLive_conn = mongodb_connection(tradbLive_str,tradbLive_db)

# Connection to  Elasticsearch
es=elasticsearch_connection(elasticsearch_conn_str)

tagsCollection=tradbLive_conn["tags"]
adsSpentCollection = tradbDev_conn["spentData"]
campaignsCollection = tradbLive_conn["campaigns"]
resortCollection = tradbLive_conn["resorts"]

def getTradbResortInfo(mongo_id):
    try:
        resortinfo = resortCollection.find_one({"_id":ObjectId(mongo_id)})
        resortName = resortinfo['resortName']
        return str(resortName)
    except:
        return ""

def getTradbTagsId(collection,campaignManagerId):
    tags_container=[]
    try:
        for return_doc in collection.find({'campaignManagerId':str(campaignManagerId)}):
            tag={}
            tag['tags']=return_doc['_id']
            tags_container.append(tag)
        return tags_container
    except:
        return tags_container
    
def getTradbTagsInfo(collection,tagsId):
    tags_json={}
    try:
        return_doc = collection.find_one({'_id':ObjectId(tagsId)})
        tags_json["campaignType"]=return_doc['contactType']
        tags_json["tag_mongoId"]=str(return_doc['_id'])
        tags_json["campaignManagerId"]=return_doc['campaignManagerId']
        tags_json["contactType"]=return_doc['contactType']
        tags_json["name"]=return_doc['name']
        tags_json["promotion"]=return_doc['promotion']
        tags_json["dnis"]=return_doc['dnis']
        tags_json["resort"]=getTradbResortInfo(str(return_doc['resort']))
        tags_json["isActive"]=return_doc['isActive']
        tags_json
        return tags_json
    except:
        return tags_json
    
def getTradbCampaignId(collection, tagId):
    try:
        return_doc = collection.find_one({"tags":{'$in' : [ObjectId(tagId)]}})
        return_doc
        return return_doc['_id']
    except :
        return None

def getTradbCampaignInfo(collection,campaignId):
    campaign_json={}
    try:
        return_doc = collection.find_one({"_id":ObjectId(campaignId)})
        campaign_json["participant"]=str(return_doc['participant'])
        campaign_json["program"]=str(return_doc['program'])
        return campaign_json
    except:
        return campaign_json
    
def calculate_spent_by_percent(totalAdSpent,percent):
    try:
        if (totalAdSpent != 0.0):
            charges = totalAdSpent * percent
            return charges
        else:
            return 0
    except Exception as e:
        print("Error:",e)

def mongodb_many_to_many_insert(collection, docs_bulk):
    try:
        if len(docs_bulk) > 0:
            x = collection.insert_many(docs_bulk)
            print(x)
            response = {"acknowledged": x.acknowledged,
                        "inserted_records": len(x.inserted_ids),
                        "records": len(docs_bulk)}
        else:
            response = {"acknowledged": False,
                        "inserted_records": 0,
                        "records": len(docs_bulk)}

    except:
        print("Error While Dumping Data into MongoDb")
        response=None

    return(response)

def adSpentElasticIndexing(es, adSpentDataBulk,fbAdSpentIndex,fbAdSpentDoctype):
    try:
        response = helpers.bulk(es, adSpentDataBulk,
                                index=fbAdSpentIndex,
                                doc_type=fbAdSpentDoctype)

        print ("\nActions RESPONSE Dumped Docs:", response[0])
        return response[0]

    except Exception as err:
        error_msg = "Elasticsearch index() ERROR:"
        print(error_msg, err)

        return None
    
def check_on_date_and_tagid(st_date,en_date,tagid,participant):
    try:
        query = {
                "query": { 
                        "bool": { 
                          "must": [
                            { "match": {"contactAttempts.tags.tag_mongoId.keyword": str(tagid)}},
                              { "match": {"campaign.participant.keyword": participant}
                            }
                          ],
                          "filter": [ 

                            { "range": { "dateCreated": { "gte": str(st_date),"lt":str(en_date)}}}
                          ]
                        }
                      }
                    }
        results = es.search(index = FbAdSpentIndex, body=query)
        hits = results["hits"]["total"]['value']
        if hits > 0:
            return False
        else:
            return True
    except Exception as err:
        print(err)

def create_date_range(days_back):
    st_date = datetime.today() - dateutil.relativedelta.relativedelta(days=days_back)
    st_date = st_date.strftime("%Y-%m-%d")
    en_date = datetime.now().strftime("%Y-%m-%d")
    print("From " + str(st_date) +" to "+ str(en_date))
    st_date = parser.parse(st_date)
    en_date = parser.parse(en_date)
    return st_date, en_date