from dags.dados.iinfra_dados import IinfraDados
from hdfs import InsecureClient
import os


class InfraDados(IinfraDados):

    def __init__(
            self,
        diretorio_datalake: str,
        path_extracao: str,
        metricas: str,
        nome_arquivo: str
    ) -> None:
        """Classe para guardar dados

        Args:
            diretorio_datalake (str): diretório do datalake (bronze, prata, ouro)
            path_extracao (str): path_extração ex: extracao_dia
            metricas (str): métrica de análise
            nome_arquivo (str): nome_arquivo
        """
        self.__cliente_hdfs = InsecureClient(
            'http://172.20.0.3:50070',
            user='hadoop'
        )
        self._diretorio_base = self.__cliente_hdfs.url
        self._diretorio_datalake = diretorio_datalake
        self._path_extracao = path_extracao
        self._metricas = metricas
        self._nome_arquivo = nome_arquivo
        self.__data = 'data'
        self._diretorio_completo = os.path.join(
            self._diretorio_base,
            self.__data,
            self._diretorio_datalake,
            self._path_extracao,
            self._metricas
        )
