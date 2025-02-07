
try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass

import json
from dados.infra_dados import InfraDados


class InfraJson(InfraDados):

    def __init__(self, diretorio_datalake, diretorio_camada, diretorio_municipio, diretorio_data, nome_arquivo):
        super().__init__(diretorio_datalake, diretorio_camada,
                         diretorio_municipio, diretorio_data, nome_arquivo)

    def __verificar_diretorio(self):
        if not os.path.exists(self._diretorio_completo):
            os.makedirs(self._diretorio_completo)

    def salvar_dados(self, **kargs):
        self.__verificar_diretorio()
        with open(os.path.join(self._diretorio_completo, self._nome_arquivo), 'a') as arquivo_json:
            json.dump(
                kargs['req'],
                arquivo_json, ensure_ascii=False
            )
            arquivo_json.write('\n')

    def carregar_dados(self):
        pass
