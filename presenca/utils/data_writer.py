import os
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any
from . import schema
from googleapiclient.http import MediaFileUpload

log = logging.getLogger(__name__)

class DataWriter:
    
    def __init__(self, config: object, gdrive_service: Any = None):
        self.config = config
        self.mode = config.MODO_EXECUCAO
        self.gdrive_service = gdrive_service
        
        if self.mode == 'local':
            self.output_path = "output"
            if not os.path.exists(self.output_path):
                os.makedirs(self.output_path)
        else:
            self.output_path = self.config.CAMINHOS['colab']['id_pasta_saida']

    def save_report_to_excel(self, report_tabs: Dict[str, pd.DataFrame], base_filename: str) -> str:
        
        try:
            data_fim = self.config.DATA_FIM_GERAL
            filename = f"{data_fim} - [StoneLab] Analytics presenca.xlsx"
        except Exception:
            time_version = datetime.now().strftime("%Y-%m-%d_%Hh%Mm")
            filename = f"relatorio_presenca_fallback_{time_version}.xlsx"
        
        if self.mode == 'local':
            return self._save_local(report_tabs, filename)
        else:
            return self._save_to_drive(report_tabs, filename)

    def _save_local(self, report_tabs: Dict[str, pd.DataFrame], filename: str) -> str:
        full_path = os.path.join(self.output_path, filename)
        
        log.info(f"Escritor de Dados: Salvando em {full_path}")
        try:
            with pd.ExcelWriter(full_path, engine='xlsxwriter') as writer:
                count = 0
                for sheet_name, df in report_tabs.items():
                    if isinstance(df, pd.DataFrame):
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        count += 1
                    else:
                        log.warning(f"Escritor de Dados: Item '{sheet_name}' não é um DataFrame, pulando...")
            
            log.info(f"Escritor de Dados: Relatório salvo com {count} abas.")
            return full_path
        
        except Exception as e:
            log.error(f"Escritor de Dados: Falha ao salvar arquivo Excel em {full_path}: {e}", exc_info=False)
            return ""

    def _save_to_drive(self, report_tabs: Dict[str, pd.DataFrame], filename: str) -> str:
        log.info(f"Escritor de Dados: Iniciando upload para Google Drive '{filename}'")
        if not self.gdrive_service:
            log.error("Escritor de Dados: Google Drive Service não inicializado. Abortando upload.")
            return ""
            
        try:
            temp_path = f"/content/{filename}"
            with pd.ExcelWriter(temp_path, engine='xlsxwriter') as writer:
                count = 0
                for sheet_name, df in report_tabs.items():
                    if isinstance(df, pd.DataFrame):
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        count += 1
            
            log.info(f"Escritor de Dados: Arquivo temporário criado com {count} abas.")

            file_metadata = {
                'name': filename,
                'parents': [self.output_path]
            }
            
            media = MediaFileUpload(temp_path, 
                                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            
            file = self.gdrive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink'
            ).execute()
            
            log.info(f"Escritor de Dados: Arquivo salvo com sucesso no Google Drive.")
            return file.get('webViewLink')

        except Exception as e:
            log.error(f"Escritor de Dados: Falha ao salvar no Google Drive: {e}", exc_info=True)
            return ""