from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
from datetime import datetime

import sys
sys.path.insert(0, '/home/boom/Documents/programming/ETL_Pipeline/UTILS')
from CONFIG import *

job_start_time = datetime.now()
job_id = job_start_time.strftime("%Y%m%d%H%M%S")


topic_name = "TestTopic"
product_group = "MyProj"

audit_json = {
    "job_id": job_id,
    "attributes": [
        {
            "name": "First Workflow",
            "value": job_id
        },
        {
            "name": "TOPIC_NAME",
            "value": topic_name
        },
        {
            "name": "PRODUCT_GROUP",
            "value": product_group
        }
    ],
    "job_start_time": job_start_time
}

stats = {}

# df_delta_raw = fnFetchDataFromDelta(topic_name, product_group, kafka_offset_collection_name, mongo_connect)
print(mongo_connect, kafka_offset_collection_name)
