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
    
    aa = req.get_json()
    
    
    conn_string_for_table = os.environ["AzureWebJobsStorage"]
    service = TableServiceClient.from_connection_string(conn_str=conn_string_for_table)
    
    # create Experiments table

    service.create_table_if_not_exists("Experiments")
    

    aa['PartitionKey'] =  aa['expID']

    aa['RowKey'] = str((datetime.datetime.max - datetime.datetime.now()).total_seconds())


    table_service_client = TableServiceClient.from_connection_string(conn_str=conn_string_for_table)
    

    try:

        tableExperiment = table_service_client.create_table(table_name="Experiments")

        tableExperiment.upsert_entity(aa)

    except:

        tableExperiment = table_service_client.get_table_client(table_name="Experiments")

        tableExperiment.upsert_entity(aa)
        

    name_ = "exp" + str(aa["expID"])

    tableRL = table_service_client.get_table_client(table_name=name_)
    my_filter = "PartitionKey eq '{}'".format(aa['expID'])
    res = tableRL.query_entities(my_filter)

    

    try:
        tableRL = table_service_client.get_table_client(table_name=name_)
        my_filter = "PartitionKey eq '{}'".format(aa['expID'])
        res = tableRL.query_entities(my_filter)
        item=res.next()
        print (item)

        return func.HttpResponse(json.dumps({"best_opition":item['best_banner']}),status_code=200)

    except:
        return func.HttpResponse(json.dumps({"best_opition": 0}),status_code=200)



