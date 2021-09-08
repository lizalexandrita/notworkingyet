from recover_xls_data import *
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='001_RAIZEN_DATA_ENG_TEST',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['raizen'],
) as dag:
    def pivot_rebuild(pvt_index):
        '''
        Function with actual pipeline
        :param pvt_index: int representing pivot index to be rebuilt
        :return: DataFrame with correct schema
        '''
        ###
        # Let's start with the generic stuff
        ###
        ws = read_worksheet(workbook="/opt/bitnami/airflow/dags/vendas-combustiveis-m3.xlsx", worksheet="Plan1")
        # nms = pivot_names(ws)  # only used to check which ones we're working with
        # print(nms)

        pvt_cache = ws._pivots[pvt_index].cache
        df_aux = df_pivot(pvt_cache)
        df = df_aux[0]
        dict_d = df_aux[1]
        table = remap(df1, dict_d)
        ex = build_schema(table)
        engine = create_engine('postgresql://nice_user:nice_password123@localhost:5432/database_plim')
        ex.to_sql('Pivot'+pvt_index, engine)
        return ex


    ###
    # Sales of oil derivative fuels by UF and product - Tabela dinâmica1 -> index 3
    ###
    t1 = PythonOperator(
        task_id='OilDerivatives_UF_Product',
        python_callable=pivot_rebuild,
        op_args=[3],
    )

    ###
    # Sales of diesel by UF and type - Tabela dinâmica3 -> index 1
    ###
    t2 = PythonOperator(
        task_id='Diesel_UF_type',
        python_callable=pivot_rebuild,
        op_args=[1],
    )

    t1
    t2
