import logging

import azure.functions as func
import numpy as np
import os


def epsilon_greedy_policy(epsilon,Q):
    if np.random.uniform(0, 1) < epsilon:
        return np.random.choice(len(Q))
    else:
        return np.argmax(Q)




def main(msg: func.QueueMessage) -> None:

    conn_string_for_table = os.environ["AzureWebJobsStorage"]
    
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))
