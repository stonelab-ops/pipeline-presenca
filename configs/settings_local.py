import os

MODO_EXECUCAO = 'local'
ANO_DO_RELATORIO = 2025
MES_DO_RELATORIO = 11

DATA_INICIO_GERAL = None
DATA_FIM_GERAL = None

BASE_DIR = os.getcwd()

CAMINHOS = {
    'local': {
        'dados_presenca': os.path.join(BASE_DIR, "raw_data_local"),
        'test_data': os.path.join(BASE_DIR, "test_data"),
        'output-dashboard': os.path.join(BASE_DIR, "output-dashboard")
    }
}