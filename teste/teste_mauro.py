import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook
from datetime import datetime
import pandas as pd
import pyodbc

# SQL_DIR = os.path.join(os.path.dirname(__file__), '../sql/sincronizar_orgaos_corporativo.sql')
SQL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sql/sincronizar_orgaos_corporativo.sql')

def carregar_query(nome_query):
    """
    Carrega a consulta correspondente ao nome_query do arquivo SQL.
    """
    with open(SQL_DIR, 'r') as file:
        sql_file = file.read()
    
    queries = sql_file.split("--")
    for query in queries:
        if query.strip().startswith(nome_query):
            return query.replace(nome_query, '').strip()
    raise ValueError(f"Query '{nome_query}' não encontrada.")

def executar_leitura_sql(hook_conn_id, nome_query):
    query = carregar_query(nome_query)
    odbc_hook = OdbcHook(odbc_conn_id=hook_conn_id)
    connection_string = odbc_hook.odbc_connection_string
    connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};{connection_string}"
    
    cnxn = pyodbc.connect(connection_string)
    df = pd.read_sql(query, cnxn)
    cnxn.close()
    return df

def comparar_e_sincronizar(**context):    
    df_orgaos_corporativo = context['ti'].xcom_pull(task_ids='obter_orgaos_corporativo')
    df_orgaos_tcm = context['ti'].xcom_pull(task_ids='obter_orgaos_tcm')

    # Renomear colunas para consistência em todos os DataFrames
    df_orgaos_corporativo = df_orgaos_corporativo.rename(columns={
        'Id_Corporativo': 'Id',
        'Descricao_Corporativo': 'Descricao',
        'Sigla_Corporativo': 'Sigla',
        'Status_Corporativo': 'Status'
    })
    df_orgaos_tcm = df_orgaos_tcm.rename(columns={
        'Id_TCM': 'Id',
        'Descricao_TCM': 'Descricao',
        'Sigla_TCM': 'Sigla',
        'Status_TCM': 'Status'
    })

    # Órgãos a serem incluídos
    novos_orgaos = df_orgaos_corporativo[~df_orgaos_corporativo['Id'].isin(df_orgaos_tcm['Id'])]
    context['ti'].xcom_push(key='novos_orgaos', value=novos_orgaos)

    # Órgãos a serem atualizados
    atualizacoes = pd.merge(
        df_orgaos_corporativo,
        df_orgaos_tcm,
        on='Id',
        suffixes=('', '_tcm')
    )
    atualizacoes = atualizacoes[
        (atualizacoes['Descricao'] != atualizacoes['Descricao_tcm']) |
        (atualizacoes['Sigla'] != atualizacoes['Sigla_tcm']) |
        (atualizacoes['Status'] != atualizacoes['Status_tcm'])
    ]

    # Selecionar apenas as colunas do corporativo para atualização
    atualizacoes = atualizacoes[['Id', 'Descricao', 'Sigla', 'Status']]
    context['ti'].xcom_push(key='atualizacoes', value=atualizacoes)

    # Órgãos a serem excluídos
    excluidos = df_orgaos_tcm[~df_orgaos_tcm['Id'].isin(df_orgaos_corporativo['Id'])]
    excluidos = excluidos[['Id', 'Descricao', 'Sigla', 'Status']]
    context['ti'].xcom_push(key='excluidos', value=excluidos)

def executar_modificacoes_sql(hook_conn_id, nome_query, registros):
    if not registros.empty:
        # Configurar a conexão ODBC
        odbc_hook = OdbcHook(odbc_conn_id=hook_conn_id)
        connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};{odbc_hook.odbc_connection_string}"
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()

        # Carregar a query do arquivo        
        query = carregar_query(nome_query).replace('\n', ' ').replace('\r', ' ').strip()

        for _, row in registros.iterrows():
            if nome_query == 'excluir_orgaos':
                # Para exclusão, só precisa do ID
                valores = (row['Id'],)
            elif nome_query == 'atualizar_orgaos':
                # Para atualização, precisa de Descricao, Sigla, Status e Id na ordem correta
                valores = (row['Descricao'], row['Sigla'], row['Status'], row['Id'])
            else:
                # Para inserção ou outro tipo de operação, se necessário
                valores = (row['Descricao'], row['Sigla'], row['Status'], row['Id'])

            # Executar a query com os valores alinhados
            cursor.execute(query, valores)

        # Confirmar alterações no banco
        cnxn.commit()
        cursor.close()
        cnxn.close()

def inserir_orgaos(hook_conn_id, **context):
    inseridos = context['ti'].xcom_pull(key='novos_orgaos', task_ids='comparar_e_sincronizar')
    executar_modificacoes_sql(hook_conn_id, 'inserir_orgaos', inseridos)

def atualizar_orgaos(hook_conn_id, **context):
    atualizados = context['ti'].xcom_pull(key='atualizacoes', task_ids='comparar_e_sincronizar')
    executar_modificacoes_sql(hook_conn_id, 'atualizar_orgaos', atualizados)

def excluir_orgaos(hook_conn_id, **context):
    excluidos = context['ti'].xcom_pull(key='excluidos', task_ids='comparar_e_sincronizar')
    executar_modificacoes_sql(hook_conn_id, 'excluir_orgaos', excluidos)

with DAG(
    'sincronizar_orgaos',
    default_args={'owner': 'airflow', 'retries': 1},
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    obter_orgaos_corporativo = PythonOperator(
        task_id='obter_orgaos_corporativo',
        python_callable=executar_leitura_sql,
        op_kwargs={'hook_conn_id': 'sqlserver_corporativo', 'nome_query': 'obter_orgaos_corporativo'}
    )

    obter_orgaos_tcm = PythonOperator(
        task_id='obter_orgaos_tcm',
        python_callable=executar_leitura_sql,
        op_kwargs={'hook_conn_id': 'sqlserver_tcm', 'nome_query': 'obter_orgaos_tcm'}
    )

    comparar_e_sincronizar_task = PythonOperator(
        task_id='comparar_e_sincronizar',
        python_callable=comparar_e_sincronizar,
        provide_context=True
    )

    inserir_novos_task = PythonOperator(
        task_id='inserir_novos',
        python_callable=inserir_orgaos,
        op_kwargs={'hook_conn_id': 'sqlserver_tcm'},
        provide_context=True
    )

    atualizar_orgaos_task = PythonOperator(
        task_id='atualizar_orgaos',
        python_callable=atualizar_orgaos,
        op_kwargs={'hook_conn_id': 'sqlserver_tcm'},
        provide_context=True
    )

    excluir_orgaos_task = PythonOperator(
        task_id='excluir_orgaos',
        python_callable=excluir_orgaos,
        op_kwargs={'hook_conn_id': 'sqlserver_tcm'},
        provide_context=True
    )

    [obter_orgaos_corporativo, obter_orgaos_tcm] >> comparar_e_sincronizar_task >> [inserir_novos_task, atualizar_orgaos_task, excluir_orgaos_task]

    if __name__ == "__main__":
        dag.test()
    