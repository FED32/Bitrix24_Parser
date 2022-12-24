import json
import requests
import psycopg2
import re
import glob

from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import numpy as np


base_url = 'http://178.154.221.218:5057'


def get_leads(date_from, date_to, url):
    """Загружает лиды через ecom API"""
    url_ = base_url + '/bitrix/get_leads'
    head = {"Content-Type": "application/json"}
    body = {
        'DATE_CREATE_FROM': date_from,
        'DATE_CREATE_TO': date_to,
        'url': url
    }
    return requests.post(url_, headers=head, data=json.dumps(body))


def get_deal_list(date_from, date_to, url):
    url_ = base_url + '/bitrix/get_deal_list'
    head = {"Content-Type": "application/json"}
    body = {
        'DATE_CREATE_FROM': date_from,
        'DATE_CREATE_TO': date_to,
        'url': url
    }
    return requests.post(url_, headers=head, data=json.dumps(body))


def test_db_connection(db_access):
    """Проверка доступа к БД"""
    try:
        conn = psycopg2.connect(db_access)
        q = conn.cursor()
        q.execute('SELECT version()')
        connection = q.fetchone()
        print(connection)
        conn.close()
        return connection
    except:
        print('Нет подключения к БД')
        return None


def get_last_date(db_params, client_id, table_name):
    """
    Возвращает последнюю дату в таблице по client_id
    """
    engine = create_engine(db_params)
    query = f"SELECT max(date_create) FROM {table_name} WHERE client_id = {client_id}"
    try:
        data = pd.read_sql(query, con=engine)
        return data.values[0][0]
    except:
        print('Произошла непредвиденная ошибка')
        return None


def get_last_date2(db_access, client_id, table_name):
    """
    Возвращает последнюю дату в таблице по client_id
    """
    query = f"SELECT max(date_create) FROM {table_name} WHERE client_id = {client_id}"
    try:
        conn = psycopg2.connect(db_access)
        q = conn.cursor()
        q.execute(query)
        result = q.fetchall()
        # print(result)
        conn.close()
        return result[0][0]
    except:
        print('Произошла непредвиденная ошибка')
        return None


def get_accounts(db_params):
    """
    Загружает из базы аккаунты
    """
    engine = create_engine(db_params)
    query = """
            SELECT 
            asd.id, 
            client_id, 
            attribute_value 
            FROM account_service_data asd 
            JOIN account_list al ON asd.account_id = al.id 
            WHERE status_1 = 'Active' 
            AND  asd.attribute_id = 21 
            ORDER BY al.client_id 
            """
    try:
        data = pd.read_sql(query, con=engine)
        print('Загружена таблица аккаунтов')
        return data
    except:
        print('Произошла непредвиденная ошибка')
        return None


def upl_to_db(db_params, dataset, table_name):
    """
    Загружает данные в БД
    """
    engine = create_engine(db_params)
    try:
        dataset.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print('Данные записаны в БД')
        return 'ok'
    except:
        print('Произошла непредвиденная ошибка')
        return None


def extract_utm(row, utm_type):
    try:
        if row is not np.nan:
            if utm_type == 'campaign':
                res = re.findall(r'utm_campaign=(.*?)&', str(row))
            elif utm_type == 'content':
                res = re.findall(r'utm_content=(.*?)&', str(row))
            elif utm_type == 'medium':
                res = re.findall(r'utm_medium=(.*?)&', str(row))
            elif utm_type == 'source':
                res = re.findall(r'utm_source=(.*?)&', str(row))
            elif utm_type == 'term':
                res = re.findall(r'utm_term=(.*?)&', str(row))
            else:
                return np.nan

            if len(res) > 0:
                return res[0]
            else:
                return np.nan
        else:
            return np.nan
    except:
        return np.nan


def extract_ya_direct(row, f_type):
    try:
        if row is not np.nan:
            if f_type == 'ad_id':
                pattern = r'ad=(.*?)&'
            elif f_type == 'banner_id':
                pattern = r'banner=(.*?)&'
            elif f_type == 'campaign_name':
                pattern = r'name=(.*?)&'
            elif f_type == 'campaign_name_lat':
                pattern = r'name_lat=(.*?)&'
            elif f_type == 'campaign_type':
                pattern = r'campaign_type=(.*?)&'
            elif f_type == 'campaign_id':
                pattern = r'campaign=(.*?)&'
            elif f_type == 'creative_id':
                pattern = r'creative=(.*?)&'
            elif f_type == 'device_type':
                pattern = r'device=(.*?)&'
            elif f_type == 'gbid':
                pattern = r'gbid=(.*?)&'
            elif f_type == 'keyword':
                pattern = r'key=(.*?)&'
            elif f_type == 'phrase_id':
                pattern = r'phrase=(.*?)&'
            elif f_type == 'retargeting_id':
                pattern = r'retargeting=(.*?)&'
            elif f_type == 'coef_goal_context_id':
                pattern = r'coef_goal_context=(.*?)&'
            elif f_type == 'interest_id':
                pattern = r'interest=(.*?)&'
            elif f_type == 'match_type':
                pattern = r'match_type=(.*?)&'
            elif f_type == 'matched_keyword':
                pattern = r'matched_keyword=(.*?)&'
            elif f_type == 'adtarget_name':
                pattern = r'adtarget_name=(.*?)&'
            elif f_type == 'adtarget_id':
                pattern = r'adtarget=(.*?)&'
            elif f_type == 'position':
                pattern = r'pos=(.*?)&'
            elif f_type == 'position_type':
                pattern = r'block=(.*?)&'
            elif f_type == 'source':
                pattern = r'source=(.*?)&'
            elif f_type == 'source_type':
                pattern = r'type=(.*?)&'
            elif f_type == 'region_name':
                pattern = r'region_name=(.*?)&'
            elif f_type == 'region_id':
                pattern = r'region=(.*?)&'
            elif f_type == 'yclid':
                pattern = r'yclid=(.*?)&'
            else:
                return np.nan

            res = re.findall(pattern, str(row))
            if len(res) > 0:
                return res[0]
            else:
                return np.nan
        else:
            return np.nan
    except:
        return np.nan


def trans_leads_data(dataset, client_id, account_id, utm_parsing=True, ya_direct_parsing=False):
    """Обрабатывает датасет"""

    columns = {'ADDRESS': 'address',
               'ADDRESS_2': 'address_2',
               'ADDRESS_CITY': 'address_city',
               'ADDRESS_COUNTRY': 'address_country',
               'ADDRESS_COUNTRY_CODE': 'address_country_code',
               'ADDRESS_LOC_ADDR_ID': 'address_loc_addr_id',
               'ADDRESS_POSTAL_CODE': 'address_postal_code',
               'ADDRESS_PROVINCE': 'address_province',
               'ADDRESS_REGION': 'address_region',
               'ASSIGNED_BY_ID': 'assigned_by_id',
               'BIRTHDATE': 'birthdate',
               'COMMENTS': 'comments',
               'COMPANY_ID': 'company_id',
               'COMPANY_TITLE': 'company_title',
               'CONTACT_ID': 'contact_id',
               'CREATED_BY_ID': 'created_by_id',
               'CURRENCY_ID': 'currency_id',
               'DATE_CLOSED': 'date_closed',
               'DATE_CREATE': 'date_create',
               'DATE_MODIFY': 'date_modify',
               'HAS_EMAIL': 'has_email',
               'HAS_IMOL': 'has_imol',
               'HAS_PHONE': 'has_phone',
               'HONORIFIC': 'honorific',
               'ID': 'id_lead',
               'IS_MANUAL_OPPORTUNITY': 'is_manual_opportunity',
               'IS_RETURN_CUSTOMER': 'is_return_customer',
               'LAST_NAME': 'last_name',
               'MODIFY_BY_ID': 'modify_by_id',
               'MOVED_BY_ID': 'moved_by_id',
               'MOVED_TIME': 'moved_time',
               'NAME': 'name',
               'OPENED': 'opened',
               'OPPORTUNITY': 'opportunity',
               'ORIGINATOR_ID': 'originator_id',
               'ORIGIN_ID': 'origin_id',
               'POST': 'post',
               'SECOND_NAME': 'second_name',
               'SOURCE_DESCRIPTION': 'source_description',
               'SOURCE_ID': 'source_id',
               'STATUS_DESCRIPTION': 'status_description',
               'STATUS_ID': 'status_id',
               'STATUS_SEMANTIC_ID': 'status_semantic_id',
               'TITLE': 'title',
               'UTM_CAMPAIGN': 'utm_campaign',
               'UTM_CONTENT': 'utm_content',
               'UTM_MEDIUM': 'utm_medium',
               'UTM_SOURCE': 'utm_source',
               'UTM_TERM': 'utm_term'}

    dtypes = {
        'address': 'str',
        'address_2': 'str',
        'address_city': 'str',
        'address_country': 'str',
        'address_country_code': 'str',
        'address_loc_addr_id': 'str',
        'address_postal_code': 'str',
        'address_province': 'str',
        'address_region': 'str',
        'assigned_by_id': 'int',
        'birthdate': 'str',
        'comments': 'str',
        'company_id': 'str',
        'company_title': 'str',
        'contact_id': 'str',
        'created_by_id': 'int',
        'currency_id': 'str',
        'date_closed': 'datetime',
        'date_create': 'datetime',
        'date_modify': 'datetime',
        'has_email': 'str',
        'has_imol': 'str',
        'has_phone': 'str',
        'honorific': 'str',
        'id_lead': 'int',
        'is_manual_opportunity': 'str',
        'is_return_customer': 'str',
        'last_name': 'str',
        'modify_by_id': 'int',
        'moved_by_id': 'int',
        'moved_time': 'datetime',
        'name': 'str',
        'opened': 'str',
        'opportunity': 'float',
        'originator_id': 'str',
        'origin_id': 'str',
        'post': 'str',
        'second_name': 'str',
        'source_description': 'str',
        'source_id': 'str',
        'status_description': 'str',
        'status_id': 'str',
        'status_semantic_id': 'str',
        'title': 'str',
        'utm_campaign': 'str',
        'utm_content': 'str',
        'utm_medium': 'str',
        'utm_source': 'str',
        'utm_term': 'str',
        'client_id': 'int',
        'account_id': 'int'
    }

    dataset.rename(columns=columns, inplace=True)

    dataset['client_id'] = client_id
    dataset['account_id'] = account_id

    dataset = dataset[list(dtypes.keys())]
    # dataset = dataset.fillna(np.nan)

    for col in dataset.columns:
        if dtypes[col] == 'int':
            dataset[col] = dataset[col].astype('int', copy=False, errors='ignore')
        elif dtypes[col] == 'float':
            dataset[col] = dataset[col].astype('float', copy=False, errors='ignore')
        elif dtypes[col] == 'datetime':
            dataset[col] = pd.to_datetime(dataset[col], errors='ignore')
        # else:
        #     dataset[col] = dataset[col].astype('str', copy=False, errors='ignore')
        # #     dataset[col] = dataset[col].fillna(np.nan)

    if utm_parsing is True:
        # dataset['utm_campaign'] = dataset['source_description'].apply(extract_utm, utm_type='campaign')
        dataset['utm_content'] = dataset['source_description'].apply(extract_utm, utm_type='content')
        dataset['utm_medium'] = dataset['source_description'].apply(extract_utm, utm_type='medium')
        dataset['utm_source'] = dataset['source_description'].apply(extract_utm, utm_type='source')
        dataset['utm_term'] = dataset['source_description'].apply(extract_utm, utm_type='term')

    if ya_direct_parsing is True:
        dataset['ya_dir_ad_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='ad_id')
        dataset['ya_dir_banner_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='banner_id')
        dataset['ya_dir_campaign_name'] = dataset['source_description'].apply(extract_ya_direct, f_type='campaign_name')
        dataset['ya_dir_campaign_name_lat'] = dataset['source_description'].apply(extract_ya_direct, f_type='campaign_name_lat')
        dataset['ya_dir_campaign_type'] = dataset['source_description'].apply(extract_ya_direct, f_type='campaign_type')
        dataset['ya_dir_campaign_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='campaign_id')
        dataset['ya_dir_creative_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='creative_id')
        dataset['ya_dir_device_type'] = dataset['source_description'].apply(extract_ya_direct, f_type='device_type')
        dataset['ya_dir_gbid'] = dataset['source_description'].apply(extract_ya_direct, f_type='gbid')
        dataset['ya_dir_keyword'] = dataset['source_description'].apply(extract_ya_direct, f_type='keyword')
        dataset['ya_dir_phrase_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='phrase_id')
        dataset['ya_dir_retargeting_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='retargeting_id')
        dataset['ya_dir_coef_goal_context_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='coef_goal_context_id')
        dataset['ya_dir_interest_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='interest_id')
        dataset['ya_dir_match_type'] = dataset['source_description'].apply(extract_ya_direct, f_type='match_type')
        dataset['ya_dir_matched_keyword'] = dataset['source_description'].apply(extract_ya_direct, f_type='matched_keyword')
        dataset['ya_dir_adtarget_name'] = dataset['source_description'].apply(extract_ya_direct, f_type='adtarget_name')
        dataset['ya_dir_adtarget_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='adtarget_id')
        dataset['ya_dir_position'] = dataset['source_description'].apply(extract_ya_direct, f_type='position')
        dataset['ya_dir_position_type'] = dataset['source_description'].apply(extract_ya_direct, f_type='position_type')
        dataset['ya_dir_source'] = dataset['source_description'].apply(extract_ya_direct, f_type='source')
        dataset['ya_dir_source_type'] = dataset['source_description'].apply(extract_ya_direct, f_type='source_type')
        dataset['ya_dir_region_name'] = dataset['source_description'].apply(extract_ya_direct, f_type='region_name')
        dataset['ya_dir_region_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='region_id')
        dataset['ya_dir_yclid'] = dataset['source_description'].apply(extract_ya_direct, f_type='yclid')

    return dataset


def trans_deals_data(dataset, client_id, account_id, utm_parsing=True, ya_direct_parsing=False):
    """Обрабатывает датасет"""

    columns = {
        'ADDITIONAL_INFO': 'additional_info',
        'ASSIGNED_BY_ID': 'assigned_by_id',
        'BEGINDATE': 'begindate',
        'CATEGORY_ID': 'category_id',
        'CLOSED': 'closed',
        'CLOSEDATE': 'closedate',
        'COMMENTS': 'comments',
        'COMPANY_ID': 'company_id',
        'CONTACT_ID': 'contact_id',
        'CREATED_BY_ID': 'created_by_id',
        'CURRENCY_ID': 'currency_id',
        'DATE_CREATE': 'date_create',
        'DATE_MODIFY': 'date_modify',
        'ID': 'id_deal',
        'IS_MANUAL_OPPORTUNITY': 'is_manual_opportunity',
        'IS_NEW': 'is_new',
        'IS_RECURRING': 'is_recurring',
        'IS_REPEATED_APPROACH': 'is_repeated_approach',
        'IS_RETURN_CUSTOMER': 'is_return_customer',
        'LAST_ACTIVITY_BY': 'last_activity_by',
        'LAST_ACTIVITY_TIME': 'last_activity_time',
        'LEAD_ID': 'lead_id',
        'LOCATION_ID': 'location_id',
        'MODIFY_BY_ID': 'modify_by_id',
        'MOVED_BY_ID': 'moved_by_id',
        'MOVED_TIME': 'moved_time',
        'OPENED': 'opened',
        'OPPORTUNITY': 'opportunity',
        'ORIGINATOR_ID': 'originator_id',
        'ORIGIN_ID': 'origin_id',
        'PROBABILITY': 'probability',
        'QUOTE_ID': 'quote_id',
        'SOURCE_DESCRIPTION': 'source_description',
        'SOURCE_ID': 'source_id',
        'STAGE_ID': 'stage_id',
        'STAGE_SEMANTIC_ID': 'stage_semantic_id',
        'TAX_VALUE': 'tax_value',
        'TITLE': 'title',
        'TYPE_ID': 'type_id',
        'UTM_CAMPAIGN': 'utm_campaign',
        'UTM_CONTENT': 'utm_content',
        'UTM_MEDIUM': 'utm_medium',
        'UTM_SOURCE': 'utm_source',
        'UTM_TERM': 'utm_term'
    }

    dtypes = {
        'additional_info': 'str',
        'assigned_by_id': 'int',
        'begindate': 'str',
        'category_id': 'str',
        'closed': 'str',
        'closedate': 'str',
        'comments': 'str',
        'company_id': 'int',
        'contact_id': 'int',
        'created_by_id': 'int',
        'currency_id': 'str',
        'date_create': 'datetime',
        'date_modify': 'datetime',
        'id_deal': 'int',
        'is_manual_opportunity': 'str' ,
        'is_new': 'str',
        'is_recurring': 'str',
        'is_repeated_approach': 'str',
        'is_return_customer': 'str',
        'last_activity_by': 'str',
        'last_activity_time': 'datetime',
        'lead_id':  'int',
        'location_id': 'str',
        'modify_by_id': 'int',
        'moved_by_id': 'int',
        'moved_time': 'datetime',
        'opened': 'str',
        'opportunity': 'float',
        'originator_id': 'str',
        'origin_id': 'str',
        'probability': 'str',
        'quote_id': 'int',
        'source_description': 'str',
        'source_id': 'str',
        'stage_id': 'str',
        'stage_semantic_id': 'str',
        'tax_value': 'str',
        'title': 'str',
        'type_id': 'str',
        'utm_campaign': 'str',
        'utm_content': 'str',
        'utm_medium': 'str',
        'utm_source': 'str',
        'utm_term': 'str',
        'client_id': 'int',
        'account_id': 'int'
    }

    dataset.rename(columns=columns, inplace=True)

    dataset['client_id'] = client_id
    dataset['account_id'] = account_id

    dataset = dataset[list(dtypes.keys())]
    # dataset = dataset.fillna(np.nan)

    for col in dataset.columns:
        if dtypes[col] == 'int':
            dataset[col] = dataset[col].astype('int', copy=False, errors='ignore')
        elif dtypes[col] == 'float':
            dataset[col] = dataset[col].astype('float', copy=False, errors='ignore')
        elif dtypes[col] == 'datetime':
            dataset[col] = pd.to_datetime(dataset[col], errors='ignore')
        # else:
        #     dataset[col] = dataset[col].astype('str', copy=False, errors='ignore')
        # #     dataset[col] = dataset[col].fillna(np.nan)

    if utm_parsing is True:
        # dataset['utm_campaign'] = dataset['source_description'].apply(extract_utm, utm_type='campaign')
        dataset['utm_content'] = dataset['source_description'].apply(extract_utm, utm_type='content')
        dataset['utm_medium'] = dataset['source_description'].apply(extract_utm, utm_type='medium')
        dataset['utm_source'] = dataset['source_description'].apply(extract_utm, utm_type='source')
        dataset['utm_term'] = dataset['source_description'].apply(extract_utm, utm_type='term')

    if ya_direct_parsing is True:
        dataset['ya_dir_ad_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='ad_id')
        dataset['ya_dir_banner_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='banner_id')
        dataset['ya_dir_campaign_name'] = dataset['source_description'].apply(extract_ya_direct,
                                                                              f_type='campaign_name')
        dataset['ya_dir_campaign_name_lat'] = dataset['source_description'].apply(extract_ya_direct,
                                                                                  f_type='campaign_name_lat')
        dataset['ya_dir_campaign_type'] = dataset['source_description'].apply(extract_ya_direct,
                                                                              f_type='campaign_type')
        dataset['ya_dir_campaign_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='campaign_id')
        dataset['ya_dir_creative_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='creative_id')
        dataset['ya_dir_device_type'] = dataset['source_description'].apply(extract_ya_direct, f_type='device_type')
        dataset['ya_dir_gbid'] = dataset['source_description'].apply(extract_ya_direct, f_type='gbid')
        dataset['ya_dir_keyword'] = dataset['source_description'].apply(extract_ya_direct, f_type='keyword')
        dataset['ya_dir_phrase_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='phrase_id')
        dataset['ya_dir_retargeting_id'] = dataset['source_description'].apply(extract_ya_direct,
                                                                               f_type='retargeting_id')
        dataset['ya_dir_coef_goal_context_id'] = dataset['source_description'].apply(extract_ya_direct,
                                                                                     f_type='coef_goal_context_id')
        dataset['ya_dir_interest_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='interest_id')
        dataset['ya_dir_match_type'] = dataset['source_description'].apply(extract_ya_direct, f_type='match_type')
        dataset['ya_dir_matched_keyword'] = dataset['source_description'].apply(extract_ya_direct,
                                                                                f_type='matched_keyword')
        dataset['ya_dir_adtarget_name'] = dataset['source_description'].apply(extract_ya_direct,
                                                                              f_type='adtarget_name')
        dataset['ya_dir_adtarget_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='adtarget_id')
        dataset['ya_dir_position'] = dataset['source_description'].apply(extract_ya_direct, f_type='position')
        dataset['ya_dir_position_type'] = dataset['source_description'].apply(extract_ya_direct,
                                                                              f_type='position_type')
        dataset['ya_dir_source'] = dataset['source_description'].apply(extract_ya_direct, f_type='source')
        dataset['ya_dir_source_type'] = dataset['source_description'].apply(extract_ya_direct, f_type='source_type')
        dataset['ya_dir_region_name'] = dataset['source_description'].apply(extract_ya_direct, f_type='region_name')
        dataset['ya_dir_region_id'] = dataset['source_description'].apply(extract_ya_direct, f_type='region_id')
        dataset['ya_dir_yclid'] = dataset['source_description'].apply(extract_ya_direct, f_type='yclid')

    return dataset

