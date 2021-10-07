import logging

import azure.functions as func
import logging

import azure.functions as func
import numpy as np
import os, uuid
import json
import ast
import datetime
from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient, TableClient



def main(req: func.HttpRequest) -> func.HttpResponse:
    
    bb = req.get_json()
    conn_string_for_table = os.environ["AzureWebJobsStorage"]
  
    table_service_client = TableServiceClient.from_connection_string(conn_str=conn_string_for_table)
    

    try:
        name_ = "exp" + str(bb["expID"])

        tableRL = table_service_client.get_table_client(table_name=name_)
        my_filter = "PartitionKey eq '{}'".format(bb['expID'])
        res = tableRL.query_entities(my_filter)
        tableRL = table_service_client.get_table_client(table_name=name_)
    
        item=res.next()
        

        return func.HttpResponse(json.dumps({"best_opition": item['best_banner'],"default" : False}),status_code=200)

    except:
        return func.HttpResponse(json.dumps({"best_opition": 0, "default" : True}),status_code=200)



