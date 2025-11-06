import os
import xml.etree.ElementTree as ET
from datetime import datetime
import pandas as pd
import gspread
import logging

log = logging.getLogger(__name__)

class DataReader:
    """Leitura de todas as fontes de dados externas."""

    def __init__(self, config, gspread_client=None):
        self.config = config
        self.gc = gspread_client
        log.info("Módulo de Leitura de Dados inicializado.")

    def load_all_sources(self) -> dict:
        """Carrega todas as fontes de dados dependendo do modo de execução."""
        mode = self.config.MODO_EXECUCAO
        log.info(f"Executando em MODO {mode.upper()}. Lendo dados: ")
        
        if mode == 'local':
            return self._load_local_sources()
        else:  
            return self._load_colab_sources()

    def _load_local_sources(self) -> dict:
        """Carrega todas as fontes de dados de arquivos CSV/XML locais."""
        test_data_path = self.config.CAMINHOS['local']['test_data']
        presenca_path = self.config.CAMINHOS['local']['dados_presenca']

        return {
            "cadastro": self._read_sheet_local(
                os.path.join(test_data_path, self.config.ARQUIVO_CADASTRO_LOCAL)
            ),
            "io_alunos": self._read_sheet_local(
                os.path.join(test_data_path, self.config.ARQUIVO_IO_LOCAL)
            ),
            "ignorar": self._read_sheet_local(
                os.path.join(test_data_path, self.config.ARQUIVO_IGNORAR_LOCAL)
            ),
            "feriados": self._read_sheet_local(
                os.path.join(test_data_path, self.config.ARQUIVO_FERIADOS_LOCAL)
            ),
            "justificativas": self._read_sheet_local(
                os.path.join(test_data_path, self.config.ARQUIVO_JUSTIFICATIVAS_LOCAL)
            ),
            "registros_brutos": self._load_all_xmls(presenca_path),
        }

    def _read_sheet_local(self, full_path: str) -> pd.DataFrame:
        """Lê um arquivo CSV do caminho completo."""
        log.info(f"Lendo arquivo local: '{full_path}'")
        if not os.path.exists(full_path):
            log.error(f"Arquivo não encontrado: '{full_path}'.")
            raise FileNotFoundError(f"Arquivo não encontrado: '{full_path}'.")
        return pd.read_csv(full_path)

    def _load_all_xmls(self, folder_path: str) -> pd.DataFrame:
        """
        Carrega os arquivos XML da pasta que correspondem 
        ao ANO_DO_RELATORIO e MES_DO_RELATORIO definidos no config.
        """
        log.info(f"Lendo arquivos XML da pasta: '{folder_path}'...")

        if not os.path.exists(folder_path):
            log.warning(f"AVISO: A pasta de dados '{folder_path}' não foi encontrada.")
            return pd.DataFrame()
        
        try:
            ano = self.config.ANO_DO_RELATORIO
            mes = self.config.MES_DO_RELATORIO
            target_pattern = f"{ano:04d}-{mes:02d}"
            log.info(f"Procurando por arquivos XML para o período: {target_pattern}")
        except AttributeError:
            log.error("Erro: ANO_DO_RELATORIO ou MES_DO_RELATORIO não foram definidos no config.py.")
            return pd.DataFrame()

        all_xml_files = [f for f in os.listdir(folder_path) if f.endswith('.xml')]
        
        files_to_load = []
        for f_name in all_xml_files:
            if target_pattern in f_name:
                files_to_load.append(os.path.join(folder_path, f_name))
        
        if not files_to_load:
            log.warning(f"AVISO: Nenhum arquivo XML encontrado para o período '{target_pattern}' na pasta '{folder_path}'.")
            log.warning("O DataFrame 'registros_brutos' estará vazio.")
            return pd.DataFrame()

        log.info(f"Encontrados {len(files_to_load)} arquivos XML para {target_pattern}.")
        df_list = [self._extract_from_xml(f) for f in files_to_load]
        
        if not df_list:
             return pd.DataFrame()
             
        return pd.concat(df_list, ignore_index=True)

    def _extract_from_xml(self, file_path: str) -> pd.DataFrame:
        """Extrai dados de presença de um único arquivo XML."""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            data = []
            ns = {'ss': 'urn:schemas-microsoft-com:office:spreadsheet'}
            rows = root.findall(".//ss:Row", ns)

            header_row_idx = next(
                (i for i, r in enumerate(rows) if 'Nome' in
                 [c.text for c in r.findall(".//ss:Data", ns)]), -1
            )
            if header_row_idx == -1:
                log.warning(f"Cabeçalho 'Nome' não encontrado em {file_path}. Pulando.")
                return pd.DataFrame()
            
            header = [c.text for c in rows[header_row_idx].findall(".//ss:Data", ns)]
            try:
                nome_idx = header.index('Nome')
                horario_idx = header.index('Horário')
            except ValueError:
                log.warning(f"Colunas 'Nome' ou 'Horário' não encontradas em {file_path}. Pulando.")
                return pd.DataFrame()
            
            for row in rows[header_row_idx + 1:]:
                cells = [c.text for c in row.findall(".//ss:Data", ns)]
                if len(cells) > nome_idx and len(cells) > horario_idx:
                    nome, horario = cells[nome_idx], cells[horario_idx]
                    if nome and horario:
                        data.append({'Name': nome.strip(), 'Datetime': horario})
            return pd.DataFrame(data)
        except ET.ParseError:
            log.error(f"Erro ao processar XML (arquivo mal formatado): {file_path}")
            return pd.DataFrame()
    
    def _load_colab_sources(self) -> dict:
        """Carrega todas as fontes de dados de Planilhas Google."""
        log.info("Carregando fontes de dados online (Colab):")
        ano = str(self.config.DATA_INICIO_GERAL.split('-')[0]) 
        presenca_path = self.config.CAMINHOS['colab']['dados_presenca']
        
        return {
            "cadastro": self._read_sheet_online(
                self.config.PLANILHA_CADASTRO, self.config.ABA_CADASTRO_PRINCIPAL
            ),
            "io_alunos": self._read_sheet_online(
                self.config.PLANILHA_IO_ALUNOS, self.config.ABA_IO_ALUNOS
            ),
            "ignorar": self._read_sheet_online(
                self.config.PLANILHA_CADASTRO, self.config.ABA_NOMES_IGNORAR
            ),
            "feriados": self._read_sheet_online(
                self.config.PLANILHA_FERIADOS, ano
            ),
            "justificativas": self._read_sheet_online(
                self.config.PLANILHA_JUSTIFICATIVAS, self.config.ABA_JUSTIFICATIVAS
            ),
            "registros_brutos": self._load_all_xmls(presenca_path),
        }

    def _read_sheet_online(self, s_name: str, a_name: str) -> pd.DataFrame:
        """Lê uma planilha do Google Sheets."""
        if not self.gc:
            log.error("Cliente GSpread (gspread_client) não foi fornecido/autenticado.")
            raise ValueError("gspread_client não inicializado.")
            
        log.info(f"Lendo planilha online: '{s_name}' | Aba: '{a_name}'...")
        worksheet = self.gc.open(s_name).worksheet(a_name)
        rows = worksheet.get_all_values()
        df = pd.DataFrame.from_records(rows[1:], columns=rows[0])
        log.info("Leitura concluída.")
        return df