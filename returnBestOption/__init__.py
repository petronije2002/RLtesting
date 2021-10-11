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
import time


def main(req: func.HttpRequest) -> func.HttpResponse:

    # ee = time.time()
    
    bb = req.get_json()
    # conn_string_for_table = os.environ["AzureWebJobsStorage"]
  
    # table_service_client = TableServiceClient.from_connection_string(conn_str=conn_string_for_table)
    

    try:
        name_ = "exp" + str(bb["expID"])

        credential = AzureNamedKeyCredential(os.environ['StorageAccountName'],os.environ['StorageKey'] )



        tc =TableClient(endpoint=os.environ['StorageAccountEndpoint'],table_name=name_,credential=credential)


        # tableRL = tc.get_table_client(table_name=name_)

        #print("here")
        my_filter = "PartitionKey eq '{}'".format(bb['expID'])
        res = tc.query_entities(my_filter,results_per_page=1)

        #print("here")
        # tableRL = table_service_client.get_table_client(table_name=name_)
    
        item=res.next()
        
        #print("total time", time.time()-ee)

        return func.HttpResponse(json.dumps({"best_opition": item['best_banner'],"default" : False}),status_code=200)

    except:
        return func.HttpResponse(json.dumps({"best_opition": 0, "default" : True}),status_code=200)



