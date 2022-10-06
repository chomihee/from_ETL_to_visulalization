import csv
import pymssql
import pyodbc
from config import CONFIG
import os
from datetime import datetime, timedelta
from datetime import time as datetime_time
from dateutil import parser
from airflow.hooks.base_hook import BaseHook
from airflow.utils.state import State
from airflow.utils.db import provide_session
from airflow.models import XCom

import pandas as pd
import numpy as np
import json
import xmltodict
import re

import requests
import psycopg2
import time
import requests
from decimal import *

file_delimeter = os.path.sep

#########################################################
# DB 정보 연동관련 테이블 불러오기
#########################################################
def get_dq_check_list() : 
    return receive_db_query_result('dw'
        , f"select * from {CONFIG['dq.fk.check.list']}", {}, as_df = True)
def get_api_table_info() : 
    return receive_db_query_result('dw'
        , f"select * from {CONFIG['meta.api.table.info']}", {}, as_df = True)
def get_api_column_info() : 
    return receive_db_query_result('dw'
        , f"select * from {CONFIG['meta.api.column.info']} order by ord", {}, as_df = True)
def get_api_invalid_column_info() : 
    return receive_db_query_result('dw'
        , f"select * from {CONFIG['meta.api.invalid_column.info']}", {}, as_df = True)
def get_api_parameters() : 
    return receive_db_query_result('dw'
        , f"select * from {CONFIG['meta.api.params.info']}", {}, as_df = True)

##########################################################
# create_folder, read_file
# - 폴더 생성 및 파일 읽는 함수
##########################################################
def create_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)
def read_file(filename):
    with open(filename, mode='rt', encoding='utf8') as f:
        v = f.read()
    return v

###########################################################
# error_log
# - 에러 로그를 기록해주는 파일
#
# - Parameter
#   - target : 대상 테이블 이름
#   - msg : 메시지 ( 새로 만들어도됨 )
#   - desc : 자세한 설명
#   - url : API 의 경우 url 입력
###########################################################
def error_log(target, msg, desc, url="NULL", **kwargs) :
    print(f'Message : {msg},  description : {desc}')
    
    # dag의 정보가 들어왓을때 로그에 해당 정보를 남기기 위해 기록
    if len(kwargs) > 0:
        dag_id = str(kwargs['dag'].dag_id)
        dag_id = f"""'{dag_id.replace("'", '"')[:min(128, len(dag_id))]}'"""

        task_id  = str(kwargs['task'].task_id)
        task_id = f"""'{task_id.replace("'", '"')[:min(128, len(task_id))]}'"""

        schedule_interval = str(kwargs['dag'].schedule_interval)
        schedule_interval = f"""'{schedule_interval.replace("'", '"')[:min(25, len(schedule_interval))]}'"""

        execution_date = f"""'{str(kwargs['execution_date'])[0:19]}'"""

    # 없을시 null로 기록
    else:
        dag_id = "NULL"
        task_id = "NULL"
        schedule_interval = "NULL"
        execution_date = "NULL"

    txt = str(desc)
    txt = txt.replace("'",'"')[:min(2000, len(txt))]

    if url != "NULL":
        url = str(url).replace("'",'"')[:min(500, len(url))]
        url = f"'{url}'"

    # 에러 기록 쿼리 생성
    query = f'''
            insert into dbo.ST_ERR_LIST (dag_id, task_id, execution_date, schedule_interval, target, msg, etl_date, decr, url)
            values ({dag_id}, {task_id}, {execution_date}, {schedule_interval}, '{target}', '{msg}', getdate(), '{txt}', {url})
            '''
    # 에러로그 기록
    execute_db_query('dw', query, {})

###########################################################
# admin_bi_2
# - ADMINBI 디비에 에어플로우를 실행하며 발생한 정보들을 적재
#
# - Parameter
#   - kwargs : Dag 에서 사용되는 kwargs
#   - table_name : 테이블명
#   - msg : 메시지 종류
#   - count : 수량 (일반적으로 적재된 데이터 수를 표기, 메시지에 따라 다른용도로 정의하여 사용 가능)
#   - task_cnt, total_cnt, total_task : 위와 동일
###########################################################
def admin_bi_2(kwargs, table_name, msg, count, task_cnt = None, total_cnt =None , total_task = None) :
    
    
    dag_name = str(kwargs['dag'])
    dag_name = dag_name[dag_name.find(" ")+1: len(dag_name)-1]
    task_id  = str(kwargs['task'])
    task_id = task_id[task_id.find(" ") + 1: len(task_id) - 1]

    execution_date = str(kwargs['execution_date'])[0:19]

    if total_cnt == -1 :
        total_cnt = count
    
    if total_task == -1 :
        total_task = task_cnt

    query = f'''
        insert into Airflow_sys_log.dbo.ADMINBI(DAG_NAME, TASK_ID, TABLE_NM, EXEC_DTM, MSG
            , CNT, TASK_CNT, TOTAL_CNT, TOTAL_TASK, ETL_CRT_DTM) 
            values ('{dag_name}', '{task_id}', '{table_name}', '{execution_date}', '{msg}'
            , '{count}', '{task_cnt}','{total_cnt}','{total_task}',getdate())
        '''
    execute_db_query('dw', query, {})


###########################################################
# parse_query, parse_multi_query
# - Query의 ${} 와 #{} 부분을 해당값으로 변환
# - Query가 여러개일 경우 parse_multi_query 를 사용 (;; 로 분류됨)
# - #{} 는 텍스트의 경우 '' 가 포함되어 생성됨 (변수와 값 비교 등에 사용)
# - ${} 의 경우 데이터 그대로 쿼리에 삽입 (테이블명 등에 사용)
#
# - Parameter
#   - query : Query 원문
#   - params : 바꿀 값들을 포함한 dict
###########################################################
def parse_query(query, params, multi = False):
    string = query
    
    # 모든 파라미터를 돌며 쿼리에 해당 파라미터가 포함되어있는지 탐색 
    for item in params.keys():
        # $ 로 시작하는것은 바로 대체
        string = string.replace("${" + item + "}", str(params[item]))
        # '#' 으로 시작할시 문자열일경우 ' 를 추가해서 대체
        tmp = str(params[item]) if isinstance(params[item], int) else '\'' + params[item] + '\''
        string = string.replace("#{" + item + "}", tmp)

    # 멀티가 아닐경우 ;; 제거
    if multi == False:
        query = query.replace(';;', '')
    
    return string


###########################################################
# get_db_connection
# - 각종 DB에 연결하는 함수
# - 연결정보는 CONFIG 에 등록 되어있음
# 
# - Parameter
#   - db : 연결될 db 명 
#   - as_dict : 출력을 딕셔너리로 사용
#   - fast : pyodbc 를 사용해 fast_executemany를 사용
###########################################################
def create_postgre_connection(host, port, user, password, db):
    return psycopg2.connect(host=host, dbname=db, user=user, password=password)

def create_connection(host, port, user, password, db, as_dict = False):
    return pymssql.connect(host + ':' + str(port), user, password, db, as_dict = as_dict)

def create_pyodbc_connection(host, port, user, password, db):
    conn_info = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + host + ';DATABASE=' + 'master' + ';UID=' + user + ';PWD=' + password
    return pyodbc.connect(f'{conn_info};TrustServerCertificate=Yes;')

def get_db_connection(db:str, as_dict = False, fast = False):
    if db == 'postgre' :
        if fast: 
            raise Exception('Postgres did not support fast_executemany')
        connections = BaseHook.get_connection(CONFIG['postgre.op.conn_id'])
        return create_postgre_connection(connections.host,
                                        connections.port,
                                        connections.login,
                                        connections.password,
                                        connections.schema)
    elif db == 'dw' :
        connections = BaseHook.get_connection(CONFIG['mssql.dw.conn_id'])
    elif db == 'app' :
        connections = BaseHook.get_connection(CONFIG['mssql.app.conn_id'])
    elif db == 'app_write' :
        connections = BaseHook.get_connection(CONFIG['mssql.app_write.conn_id'])
    elif db == 'ods' : 
        connections = BaseHook.get_connection(CONFIG['mssql.ods.conn_id'])
    elif db == 'anly' :
        connections = BaseHook.get_connection(CONFIG['mssql.anly.conn_id'])
    elif db == 'app_dev' :
        connections = BaseHook.get_connection(CONFIG['mssql.app_dev.conn_id'])
    elif db == 'app_op' :
        connections = BaseHook.get_connection(CONFIG['mssql.app_op.conn_id'])
    elif db == 'app_write_dev' :
        connections = BaseHook.get_connection(CONFIG['mssql.app_write_dev.conn_id'])
    elif db == 'app_write_op' :
        connections = BaseHook.get_connection(CONFIG['mssql.app_write_op.conn_id'])
    elif db == 'app_push_write_dev' :
        connections = BaseHook.get_connection(CONFIG['mssql.app_push_write_dev.conn_id'])
    else : 
        raise ValueError(f'Incorrect connection name : \'{db}\'')
    
    if fast :
        return create_pyodbc_connection(connections.host,
                                connections.port,
                                connections.login,
                                connections.password,
                                connections.schema)
    else :
        return create_connection(connections.host,
                                connections.port,
                                connections.login,
                                connections.password,
                                connections.schema, as_dict=as_dict)



###########################################################
# execute_db_query, execute_db_query_with_file
# - 결과가 없는 쿼리를 실행하는 함수
# - 연결정보는 CONFIG 에 등록 되어있음
# - with_file 의 경우 지정된 경로에 sql 파일이 있을경우 해당 파일의 쿼리를 사용
#
# - Parameter
#   - db : 연결될 db 명 
#   - query / filepath : 쿼리문 혹은 쿼리문이 저장된 sql 파일
#   - params : 쿼리내에 변환될 부분에 대한 딕셔너리
#   - multi : 이어진 여러개 쿼리를 동시에 사용시 사용, 각 쿼리는 ;; 로 분류됨
###########################################################

def execute_db_query_with_file(db, filepath, params, multi = False):
    query = read_file(CONFIG['sql.path'] + "/" + filepath + '.sql')
    execute_db_query(db, query, params, multi = multi)

def execute_db_query(db, query, params, multi = False):
    execute_query(get_db_connection(db), query, params, multi = multi)

def execute_query(conn, query, params, multi = False):
    try:
        cursor = conn.cursor()
        query = parse_query(query, params, multi)
        if multi : 
            subquery = query.split(';;')
            for command in subquery:
                cursor.execute(command)
                # row_count = cursor.rowcount
                # print("row_affected number : ", row_count)
        else :
            cursor.execute(query)   #테스트 필요 return : 실행 개수
        conn.commit()

    except Exception as e:
        print(query)
        raise ValueError(str(e))
    finally:
        conn.close()


###########################################################
# receive_db_query_result, receive_db_query_result_with_file
# - 결과가 있는 쿼리를 실행하고 데이터를 반환하는 함수
# - 연결정보는 CONFIG 에 등록 되어있음
# - with_file 의 경우 지정된 경로에 sql 파일이 있을경우 해당 파일의 쿼리를 사용
#
# - Parameter
#   - db : 연결될 db 명 
#   - query / filepath : 쿼리문 혹은 쿼리문이 저장된 sql 파일
#   - params : 쿼리내에 변환될 부분에 대한 딕셔너리
#   - as_df : 출력값의 데이터프레임 변환 여부
###########################################################
def receive_db_query_result_with_file(db, filepath, params, as_df = False):
    query = read_file(CONFIG['sql.path'] + "/" + filepath + '.sql')
    return receive_db_query_result(db, query, params, as_df)

def receive_db_query_result(db, query, params, as_df = False):
    return receive_query_result(get_db_connection(db), query, params, as_df)
    
def receive_query_result(conn, query, params, as_df = False):
    try:
        cursor = conn.cursor()
        query = parse_query(query, params)
        cursor.execute(query)
        results = cursor.fetchall()
        
        if as_df :
            select_result_columns = [x[0] for x in cursor.description]
            results = pd.DataFrame(results, columns=select_result_columns)

        conn.commit()
        return results

    except Exception as e:
        print('Excepted query : ' + query)
        raise ValueError(str(e))
    finally : 
        conn.close()


###########################################################
# receive_db_query_result, receive_db_query_result_with_file
# - insert 쿼리를 실행하는 함수
# - 기본적으로 execute_many 를 사용하여 업로드
# - 연결정보는 CONFIG 에 등록 되어있음
# - with_file 의 경우 지정된 경로에 sql 파일이 있을경우 해당 파일의 쿼리를 사용
#
# - Parameter
#   - db : 연결될 db 명 
#   - query / filepath : 쿼리문 혹은 쿼리문이 저장된 sql 파일
#   - params : 쿼리내에 변환될 부분에 대한 딕셔너리
#   - as_df : 출력값의 데이터프레임 변환 여부
###########################################################
def insert_db_query_with_dataset_with_file(db, filepath, dataset, params, fast = False):
    query = read_file(CONFIG['sql.path'] + file_delimeter + filepath + '.sql')
    insert_db_query_with_dataset(db, query, dataset, params, fast = fast)

def insert_db_query_with_dataset(db, query, dataset, params, fast = False):
    insert_query_with_dataset(get_db_connection(db, fast=fast), query, dataset, params, fast = fast)

def insert_query_with_dataset(conn, query, dataset, params, fast = False):
    try:
        cursor = conn.cursor()
        if fast :
            cursor.fast_executemany = True
        query = parse_query(query, params)
        cursor.executemany(query, dataset)
        conn.commit()
    except Exception as e:
        print(query)
        raise ValueError(str(e))
    finally:
        conn.close()


###########################################################
# getDate
# - 시간정보 반환함수
# - execution_date 는 기본적으로 UTC 기반으로 작동
#
# - Parameter
#   - dateformat : 출력할 스트링 형태로 아래의 형태 사용 가능
#       - 'YYYYMMDD', 'YYYY-MM-DD', 'YYYYMMDDHH', 'YYYYMMDDHHMM'
#   - kwargs : Dag에서 사용되는 kwargs 딕셔너리
#   - day : 날짜 이동 (일 기준)
###########################################################
def getDate(dateFormat, kwargs, day=0):
    # dag를 실행버튼을 눌러 수동으로 실행했을때 현재시간을 기준으로 어제날짜를 가져옴
    # 일반적으로 사용하지않음
    if 'manual__' in str(kwargs['dag_run']):
        yesterday = datetime.today() - timedelta(days=1)
        
        if dateFormat == 'YYYYMMDD':
            return str(yesterday.strftime('%Y%m%d'))
        elif dateFormat == 'YYYYMMDDHH':
            return str(yesterday.strftime('%Y%m%d%H'))
        elif dateFormat == 'YYYYMMDDHHMM':
            return str(yesterday.strftime('%Y%m%d%H%M'))
        elif dateFormat == 'YYYY-MM-DD':
            return str(yesterday.strftime('%Y-%m-%d'))
        else:
            raise ValueError("gchc_Invalid DateFormat")

    # 일반적으로 DAG에서 사용하는 기준일자를 가져오는데 사용하며 execution_date 를 기준으로 가져옴 
    else : 
        if dateFormat == 'YYYYMMDD':
            return str(kwargs['execution_date'] + timedelta(days=day)).replace('-', '')[0:8]
        elif dateFormat == 'YYYYMMDDHH':
            return str(kwargs['execution_date']).replace('-', '').replace('T', '')[0:10]
        elif dateFormat == 'YYYYMMDDHHMM':
            return str(kwargs['execution_date']).replace('-', '').replace('T', '')[0:13].replace(':', '')

        # 실제 기준일자를 출력하기 위한 부분으로 etl_date와 비교가 필요할때 사용
        elif dateFormat == 'YYYY-MM-DD':
            return str(kwargs['execution_date'] + timedelta(days=day, hours=9))[0:10]
        else:
            raise ValueError("gchc_Invalid DateFormat")





















"""
검진정보 초기적재/변경적재에서 사용
"""
def insert_process_each_rows(data_list):
    cnt = 0
    for data_items in data_list:
        rows = []
        print(data_items)
        new_data_items = reformat_data(data_items)
        rows.append(new_data_items)
        try:
            cnt = cnt + 1
            sql_path = '검진정보_초기적재/insert/ST_HTH_REC_CU_ITM_BSC_EACH_ROW'
            data = {"CU_ITM_ID":new_data_items[0],
                    "CU_REC_ID":new_data_items[1],
                    "CU_KD_CD":new_data_items[2],
                    "EXAM_ID":new_data_items[3],
                    "ABRS_VAL":new_data_items[4],
                    "ABRS_WRDS_CONT":new_data_items[5],
                    "ASM_RSLT_SLCT_YN":new_data_items[6],
                    "ITM_NMVL_ENCY_CONT":new_data_items[7],
                    "ITM_POV_ENCY_CONT":new_data_items[8],
                    "FRST_REG_DTM":new_data_items[9],
                    "FRST_REGR_TY_CD":new_data_items[10],
                    "FRST_REGR_ID":new_data_items[11],
                    "LAST_UPD_DTM":new_data_items[12],
                    "LAST_UPDR_TY_CD":new_data_items[13],
                    "LAST_UPDR_ID":new_data_items[14]}
            execute_db_query_with_file('dw', sql_path, data)
        except Exception as e:
            print(f'error : insert_process_each_rows - {cnt - 1}')
            
def reformat_data(data):
    new_data = []

    for obj in data:
        if isinstance(obj, Decimal):
            value = str(obj)
        elif isinstance(obj, datetime.datetime):
            if len(str(obj)) == 26:
                value = str(obj)[0:19]
            else:
                value = str(obj)
        else:
            value = str(obj)

        if value == 'None':
            value = ""
        elif value == 'True':
            value = 1
        elif value == 'False':
            value = 0

        new_data.append(value)

    return new_data







    """ 
    App Push 데이터 전달을 위한 API 
    """
def http_api_request(mthod: str, uri: str, body: str):
    """API 호출을 위한 목적
    Args:
        mthod (str): 통신 방식에 대한 사항
        uri (str): API uri 주소
        body (tuple): 전송 데이터 A
    Raises:
        ValueError: Errory 원인
    """
    headers = {'Content-Type': 'application/json; charset=utf-8', 'channel-code': 'DW'}
    result = ""
    if mthod == "post":
        result = http_post(uri, headers, body)
    elif mthod == "get":
        result = http_get(uri, headers, body)
    return result


def http_post(uri: str, headers: dict, body: str):
    try:
        responses = requests.post(uri, headers=headers, data=body, timeout=10)
        print("http API responses: " + str(responses.status_code) + " \n text: " + responses.text)
        result = {
            "status_code" : responses.status_code,
            "text": responses.text
        }        
        return result
    except requests.exceptions.Timeout:
        print("time out~!!!")
        print("connection error: " + requests.exceptions)                        


def http_get(uri: str, headers: dict, body: str):
    try:
        responses = requests.get(uri, headers=headers, data=body)
        print("http API responses: " + str(responses.status_code) + " \n text: " + responses.text)
        return responses.status_code
    except requests.exceptions.Timeout:
        print("time out~!!!")
        print(requests.exceptions)

