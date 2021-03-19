import os
from dotenv import load_dotenv
load_dotenv()

# Load MongoDB Environmental Variables
tradbDev_str =  os.getenv("TRADBDEV_CONN_STRING")
tradbDev_db =  os.getenv("TRADBDEV_DB")
traDataDump_str =  os.getenv("TRADBDATADUMP_CONN_STRING")
traDataDump_db =  os.getenv("TRADBDATADUMP_DB")