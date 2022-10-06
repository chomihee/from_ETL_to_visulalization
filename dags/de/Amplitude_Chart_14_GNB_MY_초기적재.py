import time

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow.utils.edgemodifier import Label

from gchclib import *
from config import CONFIG
import pprint
import datetime

data_dir = CONFIG['amplitude.path']

etl_days = 0

args = {
    'owner': 'airflow',
}

t = {}

# target_table = 'GCHC_ANLY_DM.amplitude.AMP_NEW_USER'

API_Key = '1c4759b1f0366be5e09495372ea0c75c'
Secret_Key = '19de649fdbab5f5364bebb1ef1938cf4'

with DAG(
    dag_id='Amplitude_Chart_14_GNB_MY_초기적재',
    default_args=args,
    schedule_interval='26 18 * * *',
    start_date=days_ago(2),
    tags=["TAG_AMPLITUDE"],
) as dag:

    cipher_key = BaseHook.get_connection(CONFIG['amplitude.uid.cipher_key']).password

    # Amplitude chart Id
    chart_id_20 = "6a08k2g"
    chart_id_30 = "u5krh2f"
    chart_id_40 = "9tpdnm9"
    chart_id_50 = "6gfk0qh"
    chart_id_60 = "v1xxbh6"
    chart_id_70 = "rgi8s4b"

    chart_list = [
        chart_id_20,
        chart_id_30,
        chart_id_40,
        chart_id_50,
        chart_id_60,
        chart_id_70,
    ]

    # table
    target_table = 'GCHC_ANLY_DM.amplitude.AMP_GNB_MY'

    table_list = [
        target_table
    ]

    # curl -u 1c4759b1f0366be5e09495372ea0c75c:19de649fdbab5f5364bebb1ef1938cf4 https://amplitude.com/api/3/chart/q99u9kz/query
    # curl -x '10.10.110.103:80' -u 1c4759b1f0366be5e09495372ea0c75c:19de649fdbab5f5364bebb1ef1938cf4 https://amplitude.com/api/3/chart/q99u9kz/query
    def request_chart(chartID, proxy=True):
        print('@@ request_chart', chartID)
        time.sleep(1)
        proxies = {
            'http': CONFIG['proxy.addr'],
            'https': CONFIG['proxy.addr'],
        }

        if proxy:
            response = requests.get('https://amplitude.com/api/3/chart/' + chartID + '/query', proxies=proxies,auth=(API_Key, Secret_Key))
        else:
            response = requests.get('https://amplitude.com/api/3/chart/' + chartID + '/query', auth=(API_Key, Secret_Key))
        print(response)

        return response.json()['data']


    def list_json(temp_json, types="value"):
        lst = []
        # 오늘을 제외 (-1)
        for idx in range(len(temp_json) - 1):
            if types == "value":
                lst.append(temp_json[idx]['value'])
            else:
                lst.append(temp_json[idx])
        return lst


    def retention_list_json(temp_json, num, types='value'):
        lst = []
        # 0, 1번이 같기 때문에 1부터 시작
        if types == "value":
            for idx in range(1, len(temp_json[num]['datetimes'])):
                lst.append(temp_json[num]['combined'][idx]['count'])
        elif types == "date":
            for idx in range(len(temp_json[num]['datetimes']) - 1, 0, -1):
                lst.append(temp_json[num]['datetimes'][idx])
        return lst


    def gnb(response_json):
        temp_seriesMeta = response_json['seriesMeta']
        temp_seriesCollapsed = response_json['seriesCollapsed']

        lst = []
        for i in range(len(temp_seriesMeta)):

            temp_eventIndex = 0
            temp_segments1 = None
            temp_segments2 = None
            for key, value in temp_seriesMeta[i].items():
                # print(key, value)
                if key == 'eventIndex':
                    temp_eventIndex = value
                elif key == 'segments':
                    temp_segments1 = value[0]
                    temp_segments2 = value[1]

            dct = {'EVNT_TY': temp_eventIndex}

            if temp_segments1 == "B2B":
                if temp_segments2 == "남성":
                    # sheet['D' + str(man_num)] = temp_seriesCollapsed[i][0]['value']
                    dct['SEX_CD'] = 1
                    dct['type'] = 'B2B'
                    dct['value'] = temp_seriesCollapsed[i][0]['value']
                    # dct['B2B'] = temp_seriesCollapsed[i][0]['value']
                elif temp_segments2 == "여성":
                    # sheet['D' + str(woman_num)] = temp_seriesCollapsed[i][0]['value']
                    dct['SEX_CD'] = 2
                    dct['type'] = 'B2B'
                    dct['value'] = temp_seriesCollapsed[i][0]['value']
                    # dct['B2B'] = temp_seriesCollapsed[i][0]['value']
            elif temp_segments1 == "B2C":
                if temp_segments2 == "남성":
                    # sheet['E' + str(man_num)] = temp_seriesCollapsed[i][0]['value']
                    dct['SEX_CD'] = 1
                    dct['type'] = 'B2C'
                    dct['value'] = temp_seriesCollapsed[i][0]['value']
                    # dct['B2C'] = temp_seriesCollapsed[i][0]['value']
                elif temp_segments2 == "여성":
                    # sheet['E' + str(woman_num)] = temp_seriesCollapsed[i][0]['value']
                    dct['SEX_CD'] = 2
                    dct['type'] = 'B2C'
                    dct['value'] = temp_seriesCollapsed[i][0]['value']
                    # dct['B2C'] = temp_seriesCollapsed[i][0]['value']
            else:
                # sheet['F' + str(man_num)] = temp_seriesCollapsed[i][0]['value']
                # sheet['F' + str(woman_num)] = temp_seriesCollapsed[i][0]['value']
                dct['type'] = 'NONE'
                dct['value'] = temp_seriesCollapsed[i][0]['value']
                # dct['NONE'] = temp_seriesCollapsed[i][0]['value']

            lst.append(dct)

        return lst


    def chart_gnb_my(**kwargs):
        print('@@ chart_gnb_my')
        print(f'kwargs.keys() : {kwargs.keys()}')

        df = pd.DataFrame()
        df_columns = ['AMP_DT', 'EVNT_TY', 'SEX_CD', 'AG_DV_CD', 'AMP_ID', 'B2B', 'B2C', 'NONE']

        for i in range(len(chart_list)):
            result_json = request_chart(chart_list[i])
            # pprint.pprint(result_json)

            # preprocessing
            lst = gnb(result_json)
            df_temp = pd.DataFrame(lst)

            # if len(df_temp) > 0:
            if 'SEX_CD' in df_temp:
                df_temp = df_temp.pivot(index=['EVNT_TY', 'SEX_CD'], columns='type')

                if len(df_temp.columns) == 1:
                    df_temp.columns = [str(df_temp.columns[0][1])]
                elif len(df_temp.columns) == 2:
                    df_temp.columns = ['B2B', 'B2C']
                elif len(df_temp.columns) == 3:
                    df_temp.columns = ['B2B', 'B2C', 'NONE']
                    df_temp['NONE'].fillna(0, inplace=True)

                df_temp.reset_index(inplace=True)
                # df_temp['B2B'].fillna(0, inplace=True)
                # df_temp['B2C'].fillna(0, inplace=True)
                df_temp['AG_DV_CD'] = i + 1
                df_temp['AMP_ID'] = chart_list[i]

                df = pd.concat([df, df_temp])

        if 'NONE' not in df:
            df['NONE'] = 0
            df['NONE'].fillna(0, inplace=True)
        else:
            df['NONE'].fillna(0, inplace=True)

        # df_temp = df[df['SEX_CD'].isnull()]
        # df['SEX_CD'].fillna(1, inplace=True)
        # df_temp['SEX_CD'].fillna(2, inplace=True)
        # df = pd.concat([df, df_temp])

        df['SEX_CD'].fillna(0, inplace=True)
        df['NONE'].fillna(0, inplace=True)

        df['B2B'].fillna(0, inplace=True)
        df['B2C'].fillna(0, inplace=True)

        df['EVNT_TY'] = df['EVNT_TY'].astype(int)
        df['SEX_CD'] = df['SEX_CD'].astype(int)
        df['B2B'] = df['B2B'].astype(int)
        df['B2C'] = df['B2C'].astype(int)
        df['NONE'] = df['NONE'].astype(int)

        df.sort_values(by=['EVNT_TY', 'SEX_CD'], inplace=True)

        df.reset_index(drop=True, inplace=True)
        df['AMP_DT'] = datetime.datetime.now().strftime('%Y-%m-%d')

        df = df[df_columns]

        result = insert_data(target_table=target_table, df=df)
        cnt = len(result)
        print(cnt)

        # try:
        #     admin_bi_2(kwargs, table_name='amplitude_log', msg='FROM_DATA', count=cnt, task_cnt=1, total_cnt=-1, total_task=-1)
        # except Exception as admin_bi_error:
        #     error_log('amplitude_log', 'ADMINBI_UPLOAD_ERROR', str(admin_bi_error).replace("'", ' '))


    def insert_data(target_table:str, df:"DataFrame"):
        print('@@ insert_data')
        query = """
            insert into ${target_table} ${column_tuples}
            values ${values};
            select 1;
            """
        print(f'query : {query}')
        df['etl_reg_dtm'] = 'GETDATE()'
        df_params = {
            'target_table': target_table
            , 'column_tuples': str(tuple(df.columns)).replace("\'", '')
            , 'values': make_values(df)
        }
        print(f'df_params : {df_params}')
        return receive_db_query_result('anly', query, df_params)


    def trun_data(target_table, **kwargs):
        print('target_table :' + target_table)
        print('dag.dag_id : ' + dag.dag_id)

        sql = "truncate table " + target_table

        execute_db_query('anly', sql, {})


    def make_values(df)->str:
        final_lst = []
        for row_dict in df.fillna('null').replace({False: 0, True: 1, 'None':'null'}).astype(str).to_dict(orient='records'):
            lst = [f"""\'{str(value).replace("'", "''")}\'""" for _, value in row_dict.items()]
            final_lst.append('('+','.join(lst)+')')
        string = ', '.join(final_lst)
        return string.replace('\'null\'', 'null').replace('\'GETDATE()\'', 'GETDATE()')


    start_task = BashOperator(
        task_id='start',
        bash_command='echo start...',
        do_xcom_push=False
    )

    end_task = BashOperator(
        task_id='end',
        bash_command='echo end...',
        do_xcom_push=False
    )

    chart_gnb_my = PythonOperator(
        task_id='chart_gnb_my',
        python_callable=chart_gnb_my,
        provide_context=True,
        trigger_rule='one_success',
        dag=dag
    )

    t['trun_' + target_table] = PythonOperator(
        task_id='truncate_' + target_table,
        python_callable=trun_data,
        provide_context=True,
        op_kwargs={'target_table': target_table},
        dag=dag
    )

    start_task >> Label('My 클릭') >> t['trun_' + target_table] >> chart_gnb_my >> end_task
