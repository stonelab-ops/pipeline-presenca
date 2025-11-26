import logging
import config  
import calendar
from datetime import datetime
from presenca.pipeline import PresencePipeline
from presenca.utils.data_reader import DataReader
from presenca.utils.data_writer import DataWriter

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
log = logging.getLogger(__name__)

def run_local_pipeline():
    log.info("Iniciando Pipeline - MODO: LOCAL")
    
    if config.MODO_EXECUCAO != 'local':
        log.error("Erro: MODO_EXECUCAO no config.py não está 'local'.")
        return

    try:
        ano = config.ANO_DO_RELATORIO
        mes = config.MES_DO_RELATORIO
        log.info(f"Período de Análise: {ano}-{mes:02d}")
    except Exception:
        log.error("Erro: Configuração de ANO ou MES não encontrada.")
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

    log.info(f"Pasta de dados: {config.CAMINHOS['local']['dados_presenca']}")

    data_reader = DataReader(config=config, gspread_client=None)
    data_writer = DataWriter(config=config, gdrive_service=None)
    
    pipeline = PresencePipeline(
        data_reader=data_reader,
        data_writer=data_writer,
        config=config
    )
    
    pipeline.run()

if __name__ == "__main__":
    run_local_pipeline()