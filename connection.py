import pyodbc
from pymongo import MongoClient
from bson import ObjectId
from elasticsearch import Elasticsearch, helpers
from envo import *
import pymsteams

def get_dw_conn(server,database,username,password):
    try:
        WAREHOUSE_CONN_STRING = 'DRIVER={ODBC Driver 17 for SQL Server};' \
        'SERVER=' + '{};'.format(server) + \
        'DATABASE=' + '{};'.format(database) + \
        'UID=' + '{};'.format(username) + \
        'PWD=' + '{};'.format(password)
        return pyodbc.connect(WAREHOUSE_CONN_STRING)
    except Exception as e:
        print("Error:",e)
        return None
    
def mongodb_connection(clint_connection_str,db_name):
    try:
        client = MongoClient(clint_connection_str)
        db = client[db_name]
        return db
    except Exception as e:
        print("Error:",e)
        return None

def elasticsearch_connection(connection_string):
    try:
        elasticsearch_conn = Elasticsearch([connection_string])
        return elasticsearch_conn

    except:
        error_msg = "Error occured while connecting Elasticsearch client"
        print(error_msg)
        return None

def msteamsalert_connection(connector_url):
    try:
        teamsConnector = pymsteams.connectorcard(connector_url)
        return teamsConnector
    except:
        print("Teams Connection Error")

try:
    tradbProd = mongodb_connection(TRADB_PROD_CONN_STRING,TRADB_PROD_DB)
    tradbDev = mongodb_connection(TRADB_DEV_CONN_STRING,TRADB_DEV_DB)
    
    esProd = elasticsearch_connection(ELASTICSEARCH_PROD_CONN_STRING)
    esDev = elasticsearch_connection(ELASTICSEARCH_DEV_CONN_STRING)
    
    teamsConnector =  msteamsalert_connection(TEAMS_CONNECTOR)
    dataWarehouse = get_dw_conn(SERVER,DATABASE,USERNAME,PASSWORD)
except Exception as exp:
    print("Error:",exp)