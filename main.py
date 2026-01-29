import logging
import calendar
import sys
from datetime import datetime
import schema
from presenca.pipeline import PresencePipeline
from presenca.utils.data_reader import DataReader
from presenca.utils.data_writer import DataWriter
from presenca.utils.input_validator import validar_estrutura_inputs
import gspread
from google.auth import default
from googleapiclient.discovery import build 

try:
    from configs import settings_local as config
    print(f"Modo LOCAL detectado.")
except ImportError:
    from configs import settings_colab as config
    print(f"Modo COLAB/NUVEM detectado.")

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
log = logging.getLogger(__name__)

def run_pipeline():
    mode = config.MODO_EXECUCAO
    log.info(f"Iniciando Pipeline - MODO: {mode.upper()}")
    
    try:
        ano = config.ANO_DO_RELATORIO
        mes = config.MES_DO_RELATORIO
        log.info(f"Período de Análise: {ano}-{mes:02d}")
    except Exception:
        log.error("Erro CRÍTICO: Configuração de ANO ou MES não encontrada.")
        log.error("DICA: No Colab, defina 'config.ANO_DO_RELATORIO' antes de rodar.")
        return

    try:
        data_inicio = datetime(ano, mes, 1).date()
        _, ultimo_dia = calendar.monthrange(ano, mes)
        data_fim = datetime(ano, mes, ultimo_dia).date()
        
        config.DATA_INICIO_GERAL = data_inicio.strftime('%Y-%m-%d')
        config.DATA_FIM_GERAL = data_fim.strftime('%Y-%m-%d')
    except ValueError:
        log.error(f"Erro: Data inválida para {ano}-{mes}.")
        return

    path_key = 'local' if mode == 'local' else 'colab'
    path_dados = config.CAMINHOS[path_key]['dados_presenca']
    log.info(f"Lendo dados de: {path_dados}")

    gspread_client = None
    gdrive_service = None

    if mode == 'colab':
        try:
            log.info("Autenticando sessão do Google Colab...")
            from google.colab import auth
            auth.authenticate_user()
            creds, _ = default()
            gspread_client = gspread.authorize(creds)
            gdrive_service = build('drive', 'v3', credentials=creds)
            log.info("Autenticação (Sheets + Drive) realizada com sucesso!")
        except ImportError:
            log.warning("Libs do Google Colab não encontradas.")
        except Exception as e:
            log.error(f"Erro na autenticação: {e}")
            return

    data_reader = DataReader(config=config, gspread_client=gspread_client)
    
    dados_brutos = data_reader.load_all_sources()

    if not validar_estrutura_inputs(dados_brutos):
        log.error("Pipeline interrompido na validação.")
        return

    data_writer = DataWriter(
        config=config, 
        gdrive_service=gdrive_service, 
        gspread_client=gspread_client
    )
    
    pipeline = PresencePipeline(
        data_reader=data_reader,
        data_writer=data_writer,
        config=config
    )
    
    pipeline.run(dados_input=dados_brutos)

if __name__ == "__main__":
    run_pipeline()