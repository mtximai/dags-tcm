import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook
from datetime import datetime
import pandas as pd
import pyodbc

# SQL_DIR = os.path.join(os.path.dirname(__file__), '../sql/sincronizar_orgaos_corporativo.sql')
SQL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sql/sincronizar_tipo_documento_etcm.sql')

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
    df_tipo_documento_etcm = context['ti'].xcom_pull(task_ids='obter_tipo_documento_etcm')
    df_tipo_documento_tcm = context['ti'].xcom_pull(task_ids='obter_tipo_documento_tcm')
    
    # Tipos de documento a serem incluídos
    novos_tipos_documentos = df_tipo_documento_etcm[~df_tipo_documento_etcm['Cod'].isin(df_tipo_documento_tcm['Cod'])]
    context['ti'].xcom_push(key='novos_tipos_documentos', value=novos_tipos_documentos)

    # Tipos de documento a serem atualizados
    atualizacoes = pd.merge(
        df_tipo_documento_etcm,
        df_tipo_documento_tcm,
        on='Cod',
        suffixes=('', '_tcm')
    )
    atualizacoes = atualizacoes[
        (atualizacoes['Nome'] != atualizacoes['Nome_tcm']) |
        (atualizacoes['Ativo'] != atualizacoes['Ativo_tcm']) |
        (atualizacoes['EntradaProtocolo'] != atualizacoes['EntradaProtocolo_tcm']) |
        (atualizacoes['CodTipoTramite'] != atualizacoes['CodTipoTramite_tcm']) |
        (atualizacoes['RelObrigatorio'] != atualizacoes['RelObrigatorio_tcm']) |
        (atualizacoes['ExternoTipoAssunto'] != atualizacoes['ExternoTipoAssunto_tcm']) |
        (atualizacoes['RespostaComuniProcessual'] != atualizacoes['RespostaComuniProcessual_tcm']) |
        (atualizacoes['EntradaProtocoloUsuario'] != atualizacoes['EntradaProtocoloUsuario_tcm'])
    ]    

    # Selecionar apenas as colunas do corporativo para atualização
    atualizacoes = atualizacoes[['Cod', 'Nome', 'Ativo', 'EntradaProtocolo', 'CodTipoTramite', 'RelObrigatorio', 'ExternoTipoAssunto', 'RespostaComuniProcessual', 'EntradaProtocoloUsuario']]
    context['ti'].xcom_push(key='atualizacoes', value=atualizacoes)

    # Tipos de documento a serem excluídos
    excluidos = df_tipo_documento_tcm[~df_tipo_documento_tcm['Cod'].isin(df_tipo_documento_etcm['Cod'])]
    excluidos = excluidos[['Cod', 'Nome', 'Ativo', 'EntradaProtocolo', 'CodTipoTramite', 'RelObrigatorio', 'ExternoTipoAssunto', 'RespostaComuniProcessual', 'EntradaProtocoloUsuario']]
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
            if nome_query == 'excluir_tipo_documento':
                valores = (None if pd.isna(row['Cod']) else row['Cod'],)
            elif nome_query == 'atualizar_tipo_documento':
                valores = (
                    None if pd.isna(row['Nome']) else row['Nome'],
                    None if pd.isna(row['Ativo']) else row['Ativo'],
                    None if pd.isna(row['EntradaProtocolo']) else row['EntradaProtocolo'],
                    None if pd.isna(row['CodTipoTramite']) else row['CodTipoTramite'],
                    None if pd.isna(row['RelObrigatorio']) else row['RelObrigatorio'],
                    None if pd.isna(row['ExternoTipoAssunto']) else row['ExternoTipoAssunto'],
                    None if pd.isna(row['RespostaComuniProcessual']) else row['RespostaComuniProcessual'],
                    None if pd.isna(row['EntradaProtocoloUsuario']) else row['EntradaProtocoloUsuario'],
                    None if pd.isna(row['Cod']) else row['Cod']  # 'Cod' no final para atualização
                    )
            else:  # Caso de inserção
                valores = (
                    None if pd.isna(row['Cod']) else row['Cod'],
                    None if pd.isna(row['Nome']) else row['Nome'],
                    None if pd.isna(row['Ativo']) else row['Ativo'],
                    None if pd.isna(row['EntradaProtocolo']) else row['EntradaProtocolo'],
                    None if pd.isna(row['CodTipoTramite']) else row['CodTipoTramite'],
                    None if pd.isna(row['RelObrigatorio']) else row['RelObrigatorio'],
                    None if pd.isna(row['ExternoTipoAssunto']) else row['ExternoTipoAssunto'],
                    None if pd.isna(row['RespostaComuniProcessual']) else row['RespostaComuniProcessual'],
                    None if pd.isna(row['EntradaProtocoloUsuario']) else row['EntradaProtocoloUsuario']
                    )                        

            # Executar a query com os valores alinhados
            try:
                cursor.execute(query, valores)
            except:
                print("Ocorreu um erro!") 
            

        # Confirmar alterações no banco
        cnxn.commit()
        cursor.close()
        cnxn.close()

def inserir_tipo_documento(hook_conn_id, **context):
    inseridos = context['ti'].xcom_pull(key='novos_tipos_documentos', task_ids='comparar_e_sincronizar')
    executar_modificacoes_sql(hook_conn_id, 'inserir_tipo_documento', inseridos)

def atualizar_tipo_documento(hook_conn_id, **context):
    atualizados = context['ti'].xcom_pull(key='atualizacoes', task_ids='comparar_e_sincronizar')
    executar_modificacoes_sql(hook_conn_id, 'atualizar_tipo_documento', atualizados)

def excluir_tipo_documento(hook_conn_id, **context):
    excluidos = context['ti'].xcom_pull(key='excluidos', task_ids='comparar_e_sincronizar')
    executar_modificacoes_sql(hook_conn_id, 'excluir_tipo_documento', excluidos)

with DAG(
    'sincronizar_tipo_documento_etcm',
    default_args={'owner': 'airflow', 'retries': 1},
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    obter_tipo_documento_etcm_task = PythonOperator(
        task_id='obter_tipo_documento_etcm',
        python_callable=executar_leitura_sql,
        op_kwargs={'hook_conn_id': 'sqlserver_etcm', 'nome_query': 'obter_tipo_documento_etcm'}
    )

    obter_tipo_documento_tcm_task = PythonOperator(
        task_id='obter_tipo_documento_tcm',
        python_callable=executar_leitura_sql,
        op_kwargs={'hook_conn_id': 'sqlserver_tcm', 'nome_query': 'obter_tipo_documento_tcm'}
    )

    comparar_e_sincronizar_task = PythonOperator(
        task_id='comparar_e_sincronizar',
        python_callable=comparar_e_sincronizar,
        provide_context=True
    )

    inserir_novos_task = PythonOperator(
        task_id='inserir_novos',
        python_callable=inserir_tipo_documento,
        op_kwargs={'hook_conn_id': 'sqlserver_tcm'},
        provide_context=True
    )

    atualizar_tipo_documento_task = PythonOperator(
        task_id='atualizar_tipo_documento',
        python_callable=atualizar_tipo_documento,
        op_kwargs={'hook_conn_id': 'sqlserver_tcm'},
        provide_context=True
    )

    excluir_tipo_documento_task = PythonOperator(
        task_id='excluir_tipo_documento',
        python_callable=excluir_tipo_documento,
        op_kwargs={'hook_conn_id': 'sqlserver_tcm'},
        provide_context=True
    )

    [obter_tipo_documento_etcm_task, obter_tipo_documento_tcm_task] >> comparar_e_sincronizar_task >> [inserir_novos_task, atualizar_tipo_documento_task, excluir_tipo_documento_task]

    if __name__ == "__main__":
        dag.test()