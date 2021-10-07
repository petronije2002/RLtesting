import logging

import azure.functions as func
import numpy as np
import os, uuid
import json
import ast
import datetime
from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient, TableClient

# credential = AzureNamedKeyCredential("storageaccountrl123", "z2WyAlRrsNweonBPs/uKxVZqojgeiVpa4S7gujyl42JAcCVE0MDEg2Nr7DmMk11/WVXeo61Z/VltMtf6g8SBiA==")

# service = TableServiceClient(endpoint="https://storageaccountrl123.table.core.windows.net/testTable", credential=credential)



#service = TableServiceClient.from_connection_string(conn_str=connection_string)

def epsilon_greedy_policy(epsilon,Q):
    if np.random.uniform(0, 1) < epsilon:
        return np.random.choice(len(Q))
    else:
        return np.argmax(Q)

def isThereTable(table_service_client,table_name):
    table_list = []
    for i in table_service_client.list_tables():
        table_list.append(i.name)

    if table_name in table_list:
        return True
    return False


def main(msg: func.QueueMessage) -> None:

    conn_string_for_table = os.environ["AzureWebJobsStorage"]
    service = TableServiceClient.from_connection_string(conn_str=conn_string_for_table)
    
    # create Experiments table

    service.create_table_if_not_exists("Experiments")
    print("PORUKA", msg, type(msg))

    aa=msg.get_json()

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

    print(name_)

    #test if there is a table for that experiment 

    try:
        tableRL = table_service_client.create_table(table_name=name_)

        count = list(np.zeros(int(aa['tOps'])))
        sum_rewards = list(np.zeros(int(aa['tOps'])))
        Q = list(np.zeros(int(aa['tOps'])))

        exp_row ={"PartitionKey": aa['PartitionKey'], "RowKey":aa['RowKey'],"expID": aa['expID'] ,"Q": str(Q),"sum_rewards":str(sum_rewards),"count":str(count)}

        tableRL.upsert_entity(exp_row)
    except:

        print("DFSDFSDFS")

        tableRL = table_service_client.get_table_client(table_name=name_)

        my_filter = "PartitionKey eq '{}'".format(aa['expID'])

        res = tableRL.query_entities(my_filter)

        # res = tableRL.query_entities(my_filter)

        # take the first item

        try:
            item=res.next()
            print (item)

        except:
            item=None
        

        if item != None:

            current_reward = aa['rew']

            current_banner = aa['currentOption']



            sum_rewards = np.array(ast.literal_eval(item['sum_rewards']))

            counter = np.array(ast.literal_eval(item['count']))

            Q = np.array(ast.literal_eval(item['Q']))


            sum_rewards[current_banner] += current_reward


            counter[current_banner] += 1

            Q[current_banner] = sum_rewards[current_banner]/counter[current_banner]

            banner = epsilon_greedy_policy(0.1,Q)


            new_updated_item = { "PartitionKey": item['PartitionKey'],
                             "RowKey": str((datetime.datetime.max - datetime.datetime.now()).total_seconds()),
                             'Q': str(list(Q)),
                             "count": str(list(counter)),
                             "sum_rewards": str(list(sum_rewards)),
                             "best_banner": str(banner),
                             "expID": str(aa['expID'])
                            }

            tableRL.upsert_entity(new_updated_item)

            print(new_updated_item)
            print("proposed banner",banner)


        logging.info('Python queue trigger function processed a queue item: %s',
                    msg.get_body().decode('utf-8'))
