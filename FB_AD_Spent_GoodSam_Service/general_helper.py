import logging
import petl as etl
import pandas as pd
from datetime import datetime
from requests import get,post
from dateutil import parser
import json
import pymsteams

from load_env_var import *
from connection import * 

# Connection to Mongo Tradb live and Dev
tradbDev_conn = mongodb_connection(tradbDev_str,tradbDev_db)
tradbLive_conn = mongodb_connection(tradbLive_str,tradbLive_db)
print(tradbDev_conn, "\n" ,tradbLive_conn)

# Connection to  Elasticsearch
es=elasticsearch_connection(elasticsearch_conn_str)
print(es)
# Connection to MsSql warehouse
dw_conn = get_dw_conn(server,database,username,password)
cursor = dw_conn.cursor()
print(cursor)

tagsCollection=tradbLive_conn["tags"]
adsSpentCollection = tradbDev_conn["spentData"]
campaignsCollection = tradbLive_conn["campaigns"]
resortCollection = tradbLive_conn["resorts"]

try:
    def myteamsalert(msg):
        connector_url = os.getenv("TEAMS_CONNECTOR")
        # You must create the connectorcard object with the Microsoft Webhook URL
        myTeamsMessage = pymsteams.connectorcard(connector_url)
        # Add text to the message.
        myTeamsMessage.text(msg)
    #     send the message.
        myTeamsMessage.send()
except:
    print("Error While Calling MSTeams Connector")
      
def encode_agent_name(name):
    try:
#         return name.encode('ascii', errors='ignore').strip()
        return name.decode('utf-8').strip().replace('?', '')
    except Exception as e:
        return ""
#         return str(name)
    
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

def datetime_parsing(String_date):
    try:
        d = parser.parse(String_date).date()
        return(str(d))
    except:
        print("Error while parsing datetime")

def get_serviceCharges_payload(fbData,serviceCharge):
    v=fbData
    serviceChargesJson={}
    try:
        serviceChargesJson["campaignManagerId"]=v.CampaignManagerId
        serviceChargesJson["LeadCorpWeek"]=v.CorpWeek
        serviceChargesJson["dateCreated"]=v.DateCreated
        serviceChargesJson["spent"]= float(serviceCharge) 
        serviceChargesJson["type"]= "spend"
        serviceChargesJson["subtype"]="service_charges" 
        serviceChargesJson["contactAttempts"]=getTradbTagsId(tagsCollection,int(v.CampaignManagerId))

        if len(serviceChargesJson["contactAttempts"]) > 0:
            t = serviceChargesJson["contactAttempts"]
            serviceChargesJson["campaign"]=getTradbCampaignId(campaignsCollection,str(str(t[0]["tags"])))
        else:
            serviceChargesJson["campaign"]=None
    except:
        print("Error while getting service charges payload")
    return serviceChargesJson

def get_marketingSpent_payload(fbData):
    v=fbData
    marketingSpendJson={}
    try:
        marketingSpendJson["campaignManagerId"]=v.CampaignManagerId
        marketingSpendJson["adId"]=v.adid
        marketingSpendJson["adName"]=v.AdName
        marketingSpendJson["adsetid"]=v.adsetid
        marketingSpendJson["adSetName"]=v.AdSetName
        marketingSpendJson["clicks"]=v.clicks
        marketingSpendJson["LeadCorpWeek"]=v.CorpWeek
        marketingSpendJson["dateCreated"]=v.DateCreated
        marketingSpendJson["fbCampaignId"]=v.CampaignId
        marketingSpendJson["impressions"]=v.impressions
        marketingSpendJson["spent"]=v.spent
        marketingSpendJson["subtype"]="marketing_spend"
        marketingSpendJson["type"]="spend"
        marketingSpendJson["leads"]=v.leads
        marketingSpendJson["adAccountId"]=v.ad_account_id
        marketingSpendJson["contactAttempts"]=getTradbTagsId(tagsCollection,int(v.CampaignManagerId)) 
        
        marketingSpendJson["CampaignName"]=v.CampaignName
        marketingSpendJson["participantid"]=v.dim_participantid
        marketingSpendJson["ProgramId"]=v.dimProgramId
        
        if len(marketingSpendJson["contactAttempts"]) > 0:
            t = marketingSpendJson["contactAttempts"]
            marketingSpendJson["campaign"]=getTradbCampaignId(campaignsCollection,str(str(t[0]["tags"])))
        else:
            marketingSpendJson["campaign"]=None
    except:
        print("Error while getting marketing spent payload")
    return marketingSpendJson

# Service Charges for Good Sam are 25% of ad spent value = 0.25 * spent    
def generate_goodsam_ads_spent(fb_costs):
    elasticIndexBulk=[]
    mongodbCollectionBulk=[]
    charges=0.25
    print("Good Sam Service Charges = 25%")

    for v in fb_costs.itertuples():
                
        serviceChargesJson={}
        calculatedServChar = calculate_spent_by_percent(v.spent,charges)
        serviceChargesJson = get_serviceCharges_payload(v,calculatedServChar)
        mongodbCollectionBulk.append(serviceChargesJson)
        
        marketingSpendJson=get_marketingSpent_payload(v)
        mongodbCollectionBulk.append(marketingSpendJson)
        
    return(mongodbCollectionBulk)

def check_existing_files(db_conn,participant,filename):
    dw_conn = db_conn
    existing_files = pd.read_sql("""
    SELECT *
    FROM [Warehouse01].[dbo].[fact_fbCostFilesDev]
    where 
    Participant in ('{}') 
    AND 
    filename in ('{}')
    """.format(participant,filename), dw_conn)
    existing_files = list(existing_files.filename.unique())
    if filename in existing_files:
        return True
    else:
        return False 

def get_campaign_manager_id_from_adname(ad_name):
    try:
        if "Copy" in ad_name:
            ad_name = int(ad_name.split(" ")[-3])
            return ad_name
        else:
            ad_name = int(ad_name.split(" ")[-1])
            return ad_name
    except:
        print('ERROR while fetching campaign manager id from ad name', ad_name)
        return 0