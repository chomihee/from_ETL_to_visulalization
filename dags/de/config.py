from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
# yesterday = (datetime.today()-timedelta(1)).strftime("%Y%m%d") + "18"
api_date = datetime.today().strftime('%Y%m%d%H')
search_date = (datetime.today()-timedelta(1)).strftime('%Y-%m-%d')

def get_month_array():
    today = datetime.today()
    month_array = []
    for i in range(1,13):
        month_array.append((today-relativedelta(months=i)).strftime('%Y%m'))

    return month_array

CONFIG = {
    # 개발 : DEV, 운영 : LIVE
    'run_environment' : '운영',

    # Define in Airflow -> Admin -> Connections
    'mssql.ods.conn_id': 'mssql_ods',
    'mssql.app.conn_id': 'mssql_app',
    'mssql.app_write.conn_id': 'mssql_app_write',
    'mssql.app_dev.conn_id': 'mssql_app_dev',
    'mssql.app_op.conn_id': 'mssql_app_op',
    'mssql.app_write_dev.conn_id': 'mssql_app_write_dev',
    'mssql.app_write_op.conn_id': 'mssql_app_write_op',
    'mssql.dw.conn_id': 'mssql_dw',
    'mssql.anly.conn_id': 'mssql_anly',
    'postgre.op.conn_id': 'postgre_op',
    'mssql.app_push_write_dev.conn_id': 'mssql_app_push_write_dev',
    'amplitude.uid.cipher_key': 'UID_CIPHER_KEY',

    # Path
    'log.path': '/data/gc_dp/airflow/logs',
    'sql.path': '/home/gc_dp/etl/sql',
    'amplitude.path': '/data/gc_dp/amplitude',
    'braze.path': '/data/gc_dp/braze',
    'use_proxy': True,
    'proxy.addr': '10.10.110.103:80', # 개발
    #'proxy.addr': '172.16.250.181:80', # 운영
    'max_row' : 10000,

    # Open API Meta Infomation
    'meta.api.table.info' : 'META_TABLE_INFO',
    'meta.api.column.info' : 'META_COLUMN_INFO',
    'meta.api.invalid_column.info' : 'META_INVALID_COLUMN_INFO',
    'meta.api.params.info' : 'META_API_PARAMS_INFO',

    'dq.fk.check.list' : 'airflow_sys_log.dbo.DQ_FK_CHECK_LIST',

    # Delete Time Over
    'ST.delete_date_over': 365,
    'log.delete_date_over': 180,
    'error_log.delete_date_over': 365,

    'foodsafetykorea.serviceKey' : ['f619521b1631465e90be', '6b708ade05df4f1db368'],

    # braze cluster01
    #'braze.serviceKey' : '5a65f62f-3693-430e-834b-c960a5f477c5',
    # braze cluster05
    'braze.serviceKey' : '4d34c8d4-c8d3-4382-a4f7-f61f2b0e89ca',

    'use_dblink' : True,

    'smtp.server' : '172.16.100.247',
    'smtp.port' : 25,
    'mail.id' : 'DP_admin@gchealthcare.com',

    'mail.from' : 'DP_admin@gchealthcare.com',
    'mail.to' : [
                #'0to1_kmyu@gchealthcare.com',
                '0to1_ytkim@gchealthcare.com',
                '0to1_heesung@gchealthcare.com',
                'GCSHHU@gccorp.com',
                'gckikim@gccorp.com',
                'gcmhcho@gccorp.com',
                'gcmkheo@gccorp.com'
                ],
    'mail.toreport' : ['gcmhcho@gccorp.com'],

    'params' : {
        'numOfRows' : ['100','1000','10000','100000','1000000'],
        'num_of_rows' : [ '100','500','1000','5000','10000','50000'],

        'crtrYm' : get_month_array(),

        'searchDate': [search_date],

        'yadmTp' : ['0'],

        'opCloTp' : ['0'],

        'year' : ['2009','2012','2015',
                  '2016','2017','2018','2019','2020','2021', '2022', '2023'],
        
        'mdsCd' : ['0','6','9',
                   'A','B','C','D','E','F','G','J','M','T','W'],

        'gnlNmCd' : ['1','2','3','4','5','6','8',
                    'A','B','C','D','E'],

        'areaNo' : [''],

        'sidoCd' : ['11', '21', '23', '24', '25', '26', '29'
                    '31', '32', '33', '34', '35', '36', '37', '38', '39'],

        'time' : [api_date],

        'startCreateDt' : [''],

        'sidoName' : ['전국'],

        'requestCode' : ['A41', 'A42', 'A44', 'A45', 'A46', 'A47', 'A48', 'A49'],

        'ykiho' : ['GCHC_ANLY_DW.api.DW_HR_HOSP_INF','ENCY_CVSC_SYM_VAL'],
      
        'ykiho_ori' : ['GCHC_ANLY_DW.api.DW_NH_CUI_UNI_CND','CUI_DV_ID'],


        'sickCd' : [''],

        'vcnCd' : ['GCHC_ANLY_ST.API.ST_KD_PVNT_INCT_TGT_IFTD_REL_INF_CRTE_INF', 'cd'],

        # 'nxy' : ['GCHC_ANLY_DW.pub.DW_CMM_CD', 'CD_NM', "AND GRP_CD='WEAT_LOC_INF'"]
        'nxy' : ['GCHC_ANLY_DW.DBO.DW_META_WEAT_LOC_INF', 'LOC_COORD', ""] # 기상청 좌표 CMM_CD에서 새로운 테이블로 수정
    }
    
}
