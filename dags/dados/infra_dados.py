from dags.dados.iinfra_dados import IinfraDados
from hdfs import InsecureClient
import os


class InfraDados(IinfraDados):

    def __init__(
            self,
        diretorio_datalake: str,
        diretorio_camada: str,
        diretorio_municipio: str,
        diretorio_data: str,
        nome_arquivo: str
    ) -> None:

        self.__cliente_hdfs = InsecureClient(
            'http://172.20.0.3:50070',
            user='hadoop'
        )
        self._diretorio_base = self.__cliente_hdfs.url
        self._diretorio_datalake = diretorio_datalake
        self._diretorio_camada = diretorio_camada
        self._diretorio_municipio = diretorio_municipio
        self._diretorio_data = diretorio_data
        self._nome_arquivo = nome_arquivo

        self._diretorio_completo = os.path.join(
            self._diretorio_base,
            self._diretorio_datalake,
            self._diretorio_camada,
            self._diretorio_municipio,
            self._diretorio_data
        )
