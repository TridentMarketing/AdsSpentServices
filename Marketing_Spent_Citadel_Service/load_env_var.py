from dotenv import load_dotenv
import os

load_dotenv()

# Load MongoDB Connection Environmental Variables
tradbDev_str=os.getenv("TRADBDEV_CONN_STRING")
tradbDev_db=os.getenv("TRADBDEV_DB")

tradbLive_str=os.getenv("TRADBLIVE_CONN_STRING")
tradbLive_db=os.getenv("TRADBLIVE_DB")

# Load Elasticsearch Connection Environmental Variables
elasticsearch_conn_str=os.getenv("ELASTICSEARCH_CONN_STRING")
FbAdSpentIndex=os.getenv("FB_AD_SPENT_INDEX")
FbAdSpentDoctype=os.getenv("FB_AD_SPENT_DOCTYPE")

# Load MongoDB Connection Environmental Variables
tradb_conn_str=os.getenv("TRADB_CONN_STRING")