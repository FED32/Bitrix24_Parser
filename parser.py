import logger
import json
import requests
import os
from datetime import datetime, date
from datetime import timedelta
import pandas as pd
import numpy as np
from functions import *
import warnings
warnings.filterwarnings('ignore')

############################
save_data = 0
to_db = 1

utm_parsing = True
ya_direct_parsing = True

data_folder = './data'
leads_folder = './data/leads'
deals_folder = './data/deals'

#############################

logger = logger.init_logger()

# читаем параметры подключения
host = os.environ.get('ECOMRU_PG_HOST', None)
port = os.environ.get('ECOMRU_PG_PORT', None)
ssl_mode = os.environ.get('ECOMRU_PG_SSL_MODE', None)
db_name = os.environ.get('ECOMRU_PG_DB_NAME', None)
user = os.environ.get('ECOMRU_PG_USER', None)
password = os.environ.get('ECOMRU_PG_PASSWORD', None)
target_session_attrs = 'read-write'


db_access = f"host={host} " \
            f"port={port} " \
            f"sslmode={ssl_mode} " \
            f"dbname={db_name} " \
            f"user={user} " \
            f"password={password} " \
            f"target_session_attrs={target_session_attrs}"

db_params = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

connection = test_db_connection(db_access)

if connection is not None:
    # print(connection)
    logger.info("connection to db - ok")
    # clients = get_accounts(db_params)

    # пока один клиент
    client_bitrix_url = os.environ.get('ECOMRU_BITRIX_URL', None)
    clients = pd.DataFrame([{'id': None,
                             'client_id': 30,
                             'attribute_value': client_bitrix_url}])

    for index, keys in clients.iterrows():
        account_id = keys[0]
        client_id = keys[1]
        client_url = keys[2]

        leads_last_date = get_last_date2(db_access, client_id, table_name='leads_fields_btrx')

        if leads_last_date is not None:
            leads_date_from = leads_last_date + timedelta(seconds=1)
        else:
            leads_date_from = datetime.now().replace(microsecond=0).replace(second=0) - timedelta(days=90)

        # l_date_from = '2022-01-01T23:59:00'
        l_date_from = f"{leads_date_from.date()}T{leads_date_from.time()}"
        l_date_to = f"{datetime.now().replace(microsecond=0).replace(second=0).date()}T{datetime.now().replace(microsecond=0).replace(second=0).time()}"

        leads = get_leads(date_from=l_date_from, date_to=l_date_to, url=client_url)

        if leads.status_code == 200:
            if type(leads.json()) is list:
                leads_df = pd.DataFrame(leads.json())
                # print(leads_df.shape)
                if leads_df.shape[0] > 0:
                    leads_df = trans_leads_data(dataset=leads_df,
                                                client_id=client_id,
                                                account_id=account_id,
                                                utm_parsing=utm_parsing,
                                                ya_direct_parsing=ya_direct_parsing)

                    if save_data == 1:
                        if not os.path.isdir(data_folder):
                            os.mkdir(data_folder)
                        if not os.path.isdir(leads_folder):
                            os.mkdir(leads_folder)
                        folder = f"{leads_folder}/{client_id}-{str(date.today())}"
                        if not os.path.isdir(folder):
                            os.mkdir(folder)

                        leads_df.to_csv(f"{folder}/leads_{client_id}_{str(date.today())}.csv", sep=';', index=False)
                        print('Leads saved')
                        logger.info('Leads saved')

                    if to_db == 1:
                        upl = upl_to_db(db_params, dataset=leads_df, table_name='leads_fields_btrx')
                        if upl is not None:
                            print('Leads - Upload to db successful')
                            logger.info('Leads - Upload to db successful')

                        else:
                            print('Leads - Upload to db error')
                            logger.error('Leads - Upload to db error')
                    else:
                        print('Upl to db canceled')
                        logger.info('Upl to db canceled')
                else:
                    print('No leads data for the period')
                    logger.info('No leads data for the period')
            else:
                print('Ecom API or bitrix error')
                logger.error('Ecom API or bitrix error')

        else:
            print('Ecom API error')
            logger.error('Ecom API error')

        deals_last_date = get_last_date2(db_access, client_id, table_name='deals_fields_btrx')

        if deals_last_date is not None:
            deals_date_from = deals_last_date + timedelta(seconds=1)
        else:
            deals_date_from = datetime.now().replace(microsecond=0).replace(second=0) - timedelta(days=90)

        # d_date_from = '2022-09-01T23:59:00'
        d_date_from = f"{deals_date_from.date()}T{deals_date_from.time()}"
        d_date_to = f"{datetime.now().replace(microsecond=0).replace(second=0).date()}T{datetime.now().replace(microsecond=0).replace(second=0).time()}"

        deals = get_deal_list(date_from=d_date_from, date_to=d_date_to, url=client_url)

        if deals.status_code == 200:
            if type(deals.json()) is list:
                deals_df = pd.DataFrame(deals.json())
                if deals_df.shape[0] > 0:
                    deals_df = trans_deals_data(dataset=deals_df,
                                                client_id=client_id,
                                                account_id=account_id,
                                                utm_parsing=utm_parsing,
                                                ya_direct_parsing=ya_direct_parsing)

                    if save_data == 1:
                        if not os.path.isdir(data_folder):
                            os.mkdir(data_folder)
                        if not os.path.isdir(deals_folder):
                            os.mkdir(deals_folder)
                        folder = f"{deals_folder}/{client_id}-{str(date.today())}"
                        if not os.path.isdir(folder):
                            os.mkdir(folder)

                        deals_df.to_csv(f"{folder}/deals_{client_id}_{str(date.today())}.csv", sep=';', index=False)
                        print('Deals saved')
                        logger.info('Deals saved')

                    if to_db == 1:
                        upl = upl_to_db(db_params, dataset=deals_df, table_name='deals_fields_btrx')
                        if upl is not None:
                            print('Deals - Upload to db successful')
                            logger.info('Deals - Upload to db successful')
                        else:
                            print('Deals - Upload to db error')
                            logger.error('Deals - Upload to db error')
                    else:
                        print('Upl to db canceled')
                        logger.info('Upl to db canceled')
                else:
                    print('No deals data for the period')
                    logger.info('No deals data for the period')
            else:
                print('Ecom API or bitrix error')
                logger.error('Ecom API or bitrix error')
        else:
            print('Ecom API error')
            logger.error('Ecom API error')
else:
    print('Error connection to db')
    logger.error('Error connection to db')






