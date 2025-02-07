try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass

from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from operators.tempoagoraoperator import TempoAgoraOperator
from hooks.tempo_agora_hook import TempoAgoraHook
from dados.infra_json import InfraJson
import pendulum
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


data_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_atual = pendulum.parse(data_atual)
data_atual = data_atual.strftime('%Y-%m-%d %H')
path_pasta = data_atual.replace('-', '_').replace(':', '_').replace(' ', '_')
path_pasta = f'extracao_dia_{path_pasta}'
path_pasta_spark = path_pasta


with DAG(
    dag_id='dag_extracao_tempo',
    schedule_interval=None,  # '*/30 * * * *'
    catchup=False,
    start_date=pendulum.datetime(2023, 11, 29, tz='America/Sao_Paulo')
) as dag:

    task_inicio = EmptyOperator(
        task_id='task_inicio_dag',
        dag=dag
    )

    with TaskGroup('task_tempo_atual', dag=dag) as tg_mun:
        lista_task_tempo_atual = []
        for municipio in ['ribeirao_preto']:
            extracao_previsao = TempoAgoraOperator(
                task_id=f'id_previsao_{municipio}',
                municipio=municipio,
                caminho_save_arquivos=InfraJson(
                    diretorio_datalake='datalake',
                    metricas='tempo_agora',
                    nome_arquivo='extracao.json',
                    path_extracao='dia'

                ),
                extracao='extracao'
            )
            lista_task_tempo_atual.append(extracao_previsao)

    task_fim = EmptyOperator(
        task_id='task_fim_dag',
        dag=dag,
        trigger_rule='all_done'
    )

    # open_weater_operator = SparkSubmitOperator(
    #     task_id='spark_default',
    #     application='/home/rodrigo/Documentos/projetos/spark_open_weather/spark_transform/transformacao.py',
    #     name='fazer_tranformacao',
    #     application_args=[
    #         '--path_pasta', path_pasta_spark
    #     ],
    #     trigger_rule='all_done'
    # )

    task_inicio >> task_fim
