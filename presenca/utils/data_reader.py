import os
import xml.etree.ElementTree as ET
from datetime import datetime
import pandas as pd
import gspread
import logging
import schema

log = logging.getLogger(__name__)

class DataReader:

    def __init__(self, config, gspread_client=None):
        self.config = config
        self.gc = gspread_client
        log.info("Leitor de Dados: Inicializado.")

    def load_all_sources(self) -> dict:
        mode = self.config.MODO_EXECUCAO
        log.info(f"Leitor de Dados: Executando em MODO {mode.upper()}.")
        
        if mode == 'local':
            return self._load_local_sources()
        else:
            return self._load_colab_sources()

    def _load_local_sources(self) -> dict:
        test_data_path = self.config.CAMINHOS['local']['test_data']
        presenca_path = self.config.CAMINHOS['local'].get('dados_presenca', 'raw_data_local')

        return {
            "cadastro": self._read_sheet_local(
                os.path.join(test_data_path, schema.ARQUIVO_CADASTRO_LOCAL)
            ),
            "io_alunos": self._read_sheet_local(
                os.path.join(test_data_path, schema.ARQUIVO_IO_LOCAL)
            ),
            "ignorar": self._read_sheet_local(
                os.path.join(test_data_path, schema.ARQUIVO_IGNORAR_LOCAL)
            ),
            "feriados": self._read_sheet_local(
                os.path.join(test_data_path, schema.ARQUIVO_FERIADOS_LOCAL)
            ),
            "justificativas": self._read_sheet_local(
                os.path.join(test_data_path, schema.ARQUIVO_JUSTIFICATIVAS_LOCAL)
            ),
            "registros_brutos": self._load_all_xmls(presenca_path),
        }

    def _read_sheet_local(self, full_path: str) -> pd.DataFrame:
        log.info(f"Leitor de Dados: Lendo arquivo local '{full_path}'...")
        if not os.path.exists(full_path):
            log.error(f"Leitor de Dados: Arquivo não encontrado: '{full_path}'.")
            return pd.DataFrame()
        return pd.read_csv(full_path)

    def _load_all_xmls(self, folder_path: str) -> pd.DataFrame:
        if not os.path.exists(folder_path):
            log.warning(f"Leitor de Dados: A pasta '{folder_path}' não foi encontrada.")
            return pd.DataFrame()
        
        try:
            ano = self.config.ANO_DO_RELATORIO
            mes = self.config.MES_DO_RELATORIO
            target_pattern = f"{ano:04d}-{mes:02d}"
            log.info(f"Leitor de Dados: Procurando arquivos contendo '{target_pattern}' em '{folder_path}'...")
        except AttributeError:
            log.error("Leitor de Dados: ANO_DO_RELATORIO ou MES_DO_RELATORIO não definidos.")
            return pd.DataFrame()

        files_to_load = []
        for root, dirs, files in os.walk(folder_path):
            for f_name in files:
                if f_name.endswith('.xml') and target_pattern in f_name:
                    files_to_load.append(os.path.join(root, f_name))
        
        if not files_to_load:
            log.warning(f"Leitor de Dados: Nenhum XML com padrão '{target_pattern}' encontrado.")
            return pd.DataFrame()

        log.info(f"Leitor de Dados: Encontrados {len(files_to_load)} arquivos XML válidos.")
        df_list = [self._extract_from_xml(f) for f in files_to_load]
        
        if not df_list:
             return pd.DataFrame()
             
        return pd.concat(df_list, ignore_index=True)

    def _extract_from_xml(self, file_path: str) -> pd.DataFrame:
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
                log.warning(f"Leitor de Dados: Cabeçalho 'Nome' não encontrado em {os.path.basename(file_path)}. Pulando.")
                return pd.DataFrame()
            
            header = [c.text for c in rows[header_row_idx].findall(".//ss:Data", ns)]
            try:
                nome_idx = header.index('Nome')
                horario_idx = header.index('Horário')
            except ValueError:
                log.warning(f"Leitor de Dados: Colunas obrigatórias ausentes em {os.path.basename(file_path)}. Pulando.")
                return pd.DataFrame()
            
            for row in rows[header_row_idx + 1:]:
                cells = [c.text for c in row.findall(".//ss:Data", ns)]
                if len(cells) > nome_idx and len(cells) > horario_idx:
                    nome, horario = cells[nome_idx], cells[horario_idx]
                    if nome and horario:
                        data.append({'Name': nome.strip(), 'Datetime': horario})
            return pd.DataFrame(data)
        except ET.ParseError:
            log.error(f"Leitor de Dados: XML corrompido: {file_path}")
            return pd.DataFrame()
    
    def _load_colab_sources(self) -> dict:
        log.info("Leitor de Dados: Carregando fontes de dados online (Colab)...")
        
        try:
            ano_feriado_str = str(self.config.ANO_DO_RELATORIO)
        except Exception:
            log.warning("Leitor de Dados: ANO_DO_RELATORIO não definido, usando ano atual para feriados.")
            ano_feriado_str = str(datetime.now().year)

        presenca_path = self.config.CAMINHOS['colab'].get('dados_presenca', 'raw_data')
        
        return {
            "cadastro": self._read_sheet_online(
                schema.PLANILHA_CADASTRO, schema.ABA_CADASTRO_PRINCIPAL
            ),
            "io_alunos": self._read_sheet_online(
                schema.PLANILHA_IO_ALUNOS, schema.ABA_IO_ALUNOS
            ),
            "ignorar": self._read_sheet_online(
                schema.PLANILHA_CADASTRO, schema.ABA_NOMES_IGNORAR
            ),
            "feriados": self._read_sheet_online(
                schema.PLANILHA_FERIADOS, ano_feriado_str
            ),
            "justificativas": self._read_sheet_online(
                schema.PLANILHA_JUSTIFICATIVAS, schema.ABA_JUSTIFICATIVAS
            ),
            "registros_brutos": self._load_all_xmls(presenca_path),
        }

    def _read_sheet_online(self, s_name: str, a_name: str) -> pd.DataFrame:
        if not self.gc:
            log.error("Leitor de Dados: Cliente GSpread não inicializado.")
            raise ValueError("gspread_client não inicializado.")
            
        log.info(f"Leitor de Dados: Lendo planilha online '{s_name}' | Aba: '{a_name}'...")
        worksheet = self.gc.open(s_name).worksheet(a_name)
        rows = worksheet.get_all_values()
        df = pd.DataFrame.from_records(rows[1:], columns=rows[0])
        log.info(f"Leitor de Dados: Leitura online de '{s_name}' concluída.")
        return df