import os
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional
from . import schema
from googleapiclient.http import MediaFileUpload
import gspread

try:
    from gspread_dataframe import set_with_dataframe, get_as_dataframe
    HAS_GSPREAD_DF = True
except ImportError:
    HAS_GSPREAD_DF = False

log = logging.getLogger(__name__)

class DataWriter:
    
    def __init__(self, config: object, gdrive_service: Any = None, gspread_client: Any = None):
        self.config = config
        self.mode = config.MODO_EXECUCAO
        self.gdrive_service = gdrive_service
        self.gc = gspread_client
        
        if self.mode == 'local':
            self.output_path = "output"
            if not os.path.exists(self.output_path):
                os.makedirs(self.output_path)
            
            self.dashboard_local_path = os.path.join(self.output_path, "output-dashboard")
            if not os.path.exists(self.dashboard_local_path):
                os.makedirs(self.dashboard_local_path)
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
            return self._save_local(report_tabs, filename, self.output_path)
        else:
            return self._save_to_drive(report_tabs, filename, self.output_path)

    def update_master_database(self, df_new: pd.DataFrame, spreadsheet_id: str, tab_name: str):
        if self.mode == 'local':
            self._update_local_master_db_csv(df_new)
        else:
            self._update_google_sheets_master(df_new, spreadsheet_id, tab_name)

    def _identify_date_column(self, df: pd.DataFrame) -> Optional[str]:
        if schema.OUT_COL_SEMANA in df.columns:
            return schema.OUT_COL_SEMANA
        elif schema.OUT_COL_ULTIMA_PRESENCA in df.columns:
            return schema.OUT_COL_ULTIMA_PRESENCA
        return None

    def _update_google_sheets_master(self, df_new: pd.DataFrame, spreadsheet_id: str, tab_name: str):
        log.info(f"Escritor: Atualizando DB Mestra no Google Sheets ({tab_name})...")

        if not self.gc:
            log.warning("Escritor: Cliente GSpread não disponível.")
            return
        
        if not HAS_GSPREAD_DF:
            log.error("Escritor: 'gspread-dataframe' ausente.")
            return

        try:
            sh = self.gc.open_by_key(spreadsheet_id)
            try:
                worksheet = sh.worksheet(tab_name)
            except gspread.WorksheetNotFound:
                worksheet = sh.add_worksheet(title=tab_name, rows=1000, cols=20)

            df_old = get_as_dataframe(worksheet, evaluate_formulas=True, parse_dates=True)
            df_old = df_old.dropna(how='all', axis=0).dropna(how='all', axis=1)

            date_col = self._identify_date_column(df_new)
            
            if not df_old.empty and date_col and date_col in df_old.columns:
                new_dates = df_new[date_col].astype(str).unique()
                
                df_old[date_col] = df_old[date_col].astype(str)
                df_history_clean = df_old[~df_old[date_col].isin(new_dates)].copy()
                
                df_final = pd.concat([df_history_clean, df_new], ignore_index=True)
            else:
                df_final = df_new

            set_with_dataframe(worksheet, df_final, resize=True)
            log.info("Escritor: DB Mestra (Nuvem) atualizada.")

        except Exception as e:
            log.error(f"Escritor: Erro na atualização nuvem: {e}", exc_info=True)

    def _update_local_master_db_csv(self, df_new: pd.DataFrame):
        db_filename = "STONE_LAB_DATABASE_HISTORICO.csv"
        full_path = os.path.join(self.dashboard_local_path, db_filename)
        
        log.info(f"Escritor: Atualizando Base Histórica (CSV) em {full_path}...")
        
        try:
            if os.path.exists(full_path):
                df_old = pd.read_csv(full_path)
                
                date_col = self._identify_date_column(df_new)
                
                if date_col and date_col in df_old.columns:
                    df_old[date_col] = df_old[date_col].astype(str)
                    df_new[date_col] = df_new[date_col].astype(str)
                    
                    new_dates = df_new[date_col].unique()
                    
                    df_history_clean = df_old[~df_old[date_col].isin(new_dates)].copy()
                    
                    df_final = pd.concat([df_history_clean, df_new], ignore_index=True)
                else:
                    df_final = pd.concat([df_old, df_new], ignore_index=True)
            else:
                df_final = df_new
            
            df_final.to_csv(full_path, index=False)
            
            log.info(f"Escritor: Base Histórica (CSV) atualizada. Total registros: {len(df_final)}")
            
        except Exception as e:
             log.error(f"Escritor: Erro ao atualizar CSV local: {e}")

    def _write_excel_content(self, writer, report_tabs: Dict[str, pd.DataFrame]):
        for sheet_name, df in report_tabs.items():
            if isinstance(df, pd.DataFrame):
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                if sheet_name == schema.ABA_INATIVIDADE and not df.empty:
                    self._apply_risk_formatting(writer, sheet_name, df)
            else:
                log.warning(f"Escritor: Item '{sheet_name}' inválido.")

    def _apply_risk_formatting(self, writer, sheet_name, df):
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        
        format_red = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        format_orange = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})
        format_yellow = workbook.add_format({'bg_color': '#FFFFCC', 'font_color': '#000000'})
        
        if schema.OUT_COL_RISCO not in df.columns:
            return

        risk_col_idx = df.columns.get_loc(schema.OUT_COL_RISCO)
        start_row = 1 
        end_row = len(df) + 1
        
        worksheet.conditional_format(start_row, risk_col_idx, end_row, risk_col_idx, {
            'type': 'text',
            'criteria': 'containing',
            'value': schema.RISCO_3_VERMELHO,
            'format': format_red
        })
        worksheet.conditional_format(start_row, risk_col_idx, end_row, risk_col_idx, {
            'type': 'text',
            'criteria': 'containing',
            'value': schema.RISCO_2_LARANJA,
            'format': format_orange
        })
        worksheet.conditional_format(start_row, risk_col_idx, end_row, risk_col_idx, {
            'type': 'text',
            'criteria': 'containing',
            'value': schema.RISCO_1_AMARELO,
            'format': format_yellow
        })
        
        for i, col in enumerate(df.columns):
            try:
                max_len = df[col].astype(str).map(len).max()
                column_len = max(max_len, len(col)) + 2
            except:
                column_len = 15
            worksheet.set_column(i, i, column_len)

    def _save_local(self, report_tabs: Dict[str, pd.DataFrame], filename: str, path: str) -> str:
        full_path = os.path.join(path, filename)
        log.info(f"Escritor: Salvando relatório visual (Excel) em {full_path}")
        try:
            with pd.ExcelWriter(full_path, engine='xlsxwriter') as writer:
                self._write_excel_content(writer, report_tabs)
            return full_path
        except Exception as e:
            log.error(f"Escritor: Falha no salvamento local: {e}")
            return ""

    def _save_to_drive(self, report_tabs: Dict[str, pd.DataFrame], filename: str, folder_id: str) -> str:
        log.info(f"Escritor: Upload Drive '{filename}'...")
        if not self.gdrive_service:
            log.error("Escritor: Drive Service nulo.")
            return ""
        try:
            temp_path = f"/content/{filename}"
            with pd.ExcelWriter(temp_path, engine='xlsxwriter') as writer:
                self._write_excel_content(writer, report_tabs)
            
            file_metadata = {'name': filename, 'parents': [folder_id]}
            media = MediaFileUpload(temp_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            file = self.gdrive_service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
            return file.get('webViewLink')
        except Exception as e:
            log.error(f"Escritor: Falha no upload Drive: {e}", exc_info=True)
            return ""