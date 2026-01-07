import pandas as pd
import numpy as np
import logging
import os
import unicodedata
from datetime import date
import schema

log = logging.getLogger(__name__)

class InactivityCalculator:

    def __init__(self, processed_data: dict, config: dict):
        self.registros = processed_data.get('registros_final', pd.DataFrame())
        self.config = config
        self.output_path = os.path.join("output", "output-dashboard")
        if hasattr(config, 'CAMINHOS') and 'local' in config.CAMINHOS:
             self.output_path = config.CAMINHOS['local'].get('output_dashboard', self.output_path)

    def calculate_last_presence(self, df_risk: pd.DataFrame, ref_date: date) -> pd.DataFrame:
        if not self.registros.empty:
            current_last_dates = self.registros.groupby(schema.COL_ID_STONELAB)[schema.COL_XML_DATE].max()
            current_last_dates.index = current_last_dates.index.astype(str).str.strip()
        else:
            current_last_dates = pd.Series(dtype='object')

        history_df = self._try_load_history()
        
        df_risk = self._merge_current_and_history(df_risk, current_last_dates, history_df, ref_date)
        
        return self._finalize_days_calculation(df_risk, ref_date)

    def _try_load_history(self) -> pd.DataFrame:
        try:
            filename = "STONE_LAB_DATABASE_HISTORICO.csv"
            path = os.path.join(self.output_path, filename)
            
            if os.path.exists(path):
                df = pd.read_csv(path, dtype=str)
                
                col_freq_obs = self._find_col(df, [schema.DB_HIST_COL_FREQ_OBS])
                col_situacao = self._find_col(df, [schema.DB_HIST_COL_SITUACAO])
                
                if col_freq_obs and col_situacao:
                    df['temp_freq'] = pd.to_numeric(df[col_freq_obs], errors='coerce').fillna(0)
                    df['temp_sit'] = df[col_situacao].astype(str).str.strip()
                    
                    mask_valid = (df['temp_freq'] > 0) | (df['temp_sit'] == schema.STATUS_JUSTIFICADO)
                    
                    df_filtered = df[mask_valid].copy()
                    return df_filtered
                
                return df
                
        except Exception as e:
            log.warning(f"Calculator: Erro ao ler histórico: {e}")
        
        return pd.DataFrame()

    def _merge_current_and_history(self, df: pd.DataFrame, current_dates: pd.Series, 
                                   history_df: pd.DataFrame, ref_date: date) -> pd.DataFrame:
        hist_by_key = pd.Series(dtype='object')
        hist_by_name = pd.Series(dtype='object')

        if not history_df.empty:
            col_hist_key = self._find_col(history_df, [schema.DB_HIST_COL_ID, 'id_stonelab', 'ID'])
            col_hist_date = self._find_col(history_df, [schema.DB_HIST_COL_DATE, 
                                                        schema.OUT_COL_ULTIMA_PRESENCA, 'Date'])
            
            if col_hist_key and col_hist_date:
                history_df['temp_date_parsed'] = pd.to_datetime(history_df[col_hist_date], errors='coerce')
                
                ref_ts = pd.Timestamp(ref_date)
                history_valid = history_df[
                    (history_df['temp_date_parsed'].notna()) & 
                    (history_df['temp_date_parsed'] <= ref_ts)
                ].copy()

                history_valid['key_norm'] = self._normalize_string(history_valid[col_hist_key])
                hist_by_key = history_valid.groupby('key_norm')['temp_date_parsed'].max()
                
                col_hist_name = self._find_col(history_df, [schema.DB_HIST_COL_NOME, 'Nome', 'name'])
                if col_hist_name:
                    history_valid['name_norm'] = self._normalize_string(history_valid[col_hist_name])
                    hist_by_name = history_valid.groupby('name_norm')['temp_date_parsed'].max()

        df['temp_id_norm'] = self._normalize_string(df[schema.COL_ID_STONELAB])
        
        col_nome = schema.COL_NAME if schema.COL_NAME in df.columns else 'Nome'
        has_name = col_nome in df.columns
        if has_name:
            df['temp_name_norm'] = self._normalize_string(df[col_nome])

        def get_best_date(row):
            sid = str(row[schema.COL_ID_STONELAB]).strip()
            if sid in current_dates.index:
                return current_dates[sid]
            
            id_norm = row['temp_id_norm']
            if id_norm in hist_by_key.index:
                return hist_by_key[id_norm]

            if has_name:
                name_norm = row['temp_name_norm']
                if name_norm in hist_by_name.index:
                    return hist_by_name[name_norm]
            
            return pd.NaT

        df[schema.OUT_COL_ULTIMA_PRESENCA] = df.apply(get_best_date, axis=1)
        
        drop_cols = ['temp_id_norm']
        if has_name: drop_cols.append('temp_name_norm')
        df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)
        
        return df

    def _finalize_days_calculation(self, df: pd.DataFrame, ref_date: date) -> pd.DataFrame:
        if schema.OUT_COL_ULTIMA_PRESENCA not in df.columns:
            df[schema.OUT_COL_ULTIMA_PRESENCA] = pd.NaT

        def calculate_days(row):
            last_pres = row[schema.OUT_COL_ULTIMA_PRESENCA]
            
            if pd.isna(last_pres):
                if 'io_start_date' in row and pd.notna(row['io_start_date']):
                    start_date_io = row['io_start_date']
                    if isinstance(start_date_io, pd.Timestamp): start_date_io = start_date_io.date()
                    
                    if start_date_io > ref_date: 
                        return 0 
                    
                    days_since_start = (ref_date - start_date_io).days
                    return max(0, days_since_start)
                return 999 
            
            if isinstance(last_pres, pd.Timestamp): last_pres = last_pres.date()
            elif isinstance(last_pres, str): last_pres = pd.to_datetime(last_pres).date()
            
            days = (ref_date - last_pres).days
            return max(0, days)

        df[schema.OUT_COL_DIAS_AUSENTE] = df.apply(calculate_days, axis=1)
        return df

    @staticmethod
    def _normalize_string(series: pd.Series) -> pd.Series:
        s = series.astype(str).str.upper().str.strip().str.replace(r'\.0$', '', regex=True)
        return s.apply(lambda x: ''.join(c for c in unicodedata.normalize('NFD', x)
                                          if unicodedata.category(c) != 'Mn'))

    @staticmethod
    def _find_col(df, options):
        cols_lower = {c.lower(): c for c in df.columns}
        for opt in options:
            if opt.lower() in cols_lower:
                return cols_lower[opt.lower()]
        return None