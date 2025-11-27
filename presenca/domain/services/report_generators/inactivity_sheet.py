import pandas as pd
import numpy as np
import logging
import os
from datetime import date
from typing import Dict, List, Set, Any
from ....utils import schema
from ...models.coordinator import Coordinator

log = logging.getLogger(__name__)

class InactivitySheetGenerator:
    
    def __init__(self, processed_data: dict, config: dict):
        self.registros = processed_data.get('registros_final', pd.DataFrame())
        self.cadastro = processed_data.get('cadastro', pd.DataFrame())
        self.tenures = processed_data.get('tenures', {})
        self.justificativas_raw = processed_data.get('justificativas', pd.DataFrame())
        self.config = config

    def generate(self) -> dict:
        log.info("Gerador Inatividade: Calculando riscos...")
        
        try:
            ref_date = pd.to_datetime(self.config.DATA_FIM_GERAL).date()
            start_month = pd.to_datetime(self.config.DATA_INICIO_GERAL).date()
        except Exception:
            log.error("Gerador Inatividade: Datas de configuração inválidas.")
            return {schema.ABA_INATIVIDADE: pd.DataFrame()}
        
        active_ids = self._get_active_students_ids(start_month, ref_date)
        
        if not active_ids:
            return {schema.ABA_INATIVIDADE: pd.DataFrame()}

        justifications_map = self._prepare_justifications_map()

        df_risk = self.cadastro[
            self.cadastro[schema.COL_ID_STONELAB].astype(str).isin(active_ids)
        ].copy()

        col_ativo = getattr(schema, 'CADASTRO_ATIVO', 'Ativo')
        if col_ativo in df_risk.columns:
            df_risk['ativo_temp'] = pd.to_numeric(df_risk[col_ativo], errors='coerce').fillna(0)
            df_risk = df_risk[df_risk['ativo_temp'] == 1].copy()
            df_risk.drop(columns=['ativo_temp'], inplace=True)
            log.info(f"Filtro de Ativos aplicado. Total alunos analisados: {len(df_risk)}")
        else:
            log.warning(f"Coluna '{col_ativo}' não encontrada no cadastro. Filtro ignorado.")

        df_risk['io_start_date'] = df_risk[schema.COL_ID_STONELAB].astype(str).map(self._get_start_dates_map())

        df_risk = self._calculate_metrics(df_risk, ref_date)
        
        df_final = self._classify_and_format(df_risk, justifications_map, ref_date)
        
        return {schema.ABA_INATIVIDADE: df_final}
    def _normalize_string(self, series: pd.Series) -> pd.Series:
        return series.astype(str).str.upper().str.strip().str.replace(r'\.0$', '', regex=True)

    def _prepare_justifications_map(self) -> Dict[str, Set[date]]:
        if self.justificativas_raw.empty:
            return {}

        df_just = self.justificativas_raw.copy()
        df_just.columns = df_just.columns.str.strip()
        col_map = {
            schema.JUSTIFICATIVA_INICIO: schema.COL_START,
            schema.JUSTIFICATIVA_FIM: schema.COL_END,
            schema.JUSTIFICATIVA_ID_STONELAB: schema.COL_ID_STONELAB
        }
        df_just.rename(columns={k: v for k, v in col_map.items() if k in df_just.columns}, inplace=True)

        required = [schema.COL_START, schema.COL_END, schema.COL_ID_STONELAB]
        if not all(c in df_just.columns for c in required):
            return {}

        df_just[schema.COL_START] = pd.to_datetime(df_just[schema.COL_START], errors='coerce').dt.date
        df_just[schema.COL_END] = pd.to_datetime(df_just[schema.COL_END], errors='coerce').dt.date
        df_just.dropna(subset=required, inplace=True)

        just_map = {}
        for row in df_just.itertuples():
            sid = str(getattr(row, schema.COL_ID_STONELAB))
            start = getattr(row, schema.COL_START)
            end = getattr(row, schema.COL_END)
            if start > end: continue
            if sid not in just_map: just_map[sid] = set()
            try:
                just_map[sid].update(pd.date_range(start, end).date)
            except: pass
        return just_map

    def _get_freq_value(self, tenure_obj: Any) -> int:
        candidates = ['frequency', 'frequency_1', 'freq', 'freq_1', 'expected_frequency', 'original_expected_frequency']
        for attr in candidates:
            if hasattr(tenure_obj, attr):
                val = getattr(tenure_obj, attr)
                try:
                    return int(float(val))
                except:
                    continue
        return 1 

    def _get_active_students_ids(self, start_date: date, end_date: date) -> List[str]:
        active_ids = []
        for sid, tenure_list in self.tenures.items():
            is_active = False
            for tenure in tenure_list:
                if self._get_freq_value(tenure) < 1:
                    continue
                if tenure.end and tenure.end < end_date:
                    continue
                t_end = tenure.end if tenure.end else date.max
                if not (tenure.beginning > end_date or t_end < start_date):
                    is_active = True
                    break
            if is_active:
                active_ids.append(sid)
        return active_ids

    def _get_start_dates_map(self) -> Dict[str, date]:
        start_map = {}
        for sid, tenure_list in self.tenures.items():
            valid_starts = []
            for t in tenure_list:
                if self._get_freq_value(t) >= 1:
                    valid_starts.append(t.beginning)
            if valid_starts:
                start_map[str(sid)] = min(valid_starts)
        return start_map
    def _try_load_history(self) -> pd.DataFrame:
        try:
            path = os.path.join("output", "output-dashboard", "STONE_LAB_DATABASE_HISTORICO.csv")
            if os.path.exists(path):
                df = pd.read_csv(path)
                cols_lower = {c.lower(): c for c in df.columns}
                if 'semana' in cols_lower and schema.OUT_COL_ULTIMA_PRESENCA not in df.columns:
                    df.rename(columns={cols_lower['semana']: schema.OUT_COL_ULTIMA_PRESENCA}, inplace=True)
                if 'nome' in cols_lower and schema.COL_ID_STONELAB not in df.columns:
                    df.rename(columns={cols_lower['nome']: 'temp_name_id'}, inplace=True)
                log.info(f"Inatividade: Histórico lido ({len(df)} linhas).")
                return df
            else:
                log.info("Inatividade: Nenhum histórico local encontrado.")
        except Exception as e:
            log.warning(f"Inatividade: Erro ao ler histórico: {e}")
        return pd.DataFrame()
    def _calculate_metrics(self, df: pd.DataFrame, ref_date: date) -> pd.DataFrame:
        if not self.registros.empty:
            current_last_dates = self.registros.groupby(schema.COL_ID_STONELAB)[schema.COL_XML_DATE].max()
            current_last_dates.index = current_last_dates.index.astype(str)
        else:
            current_last_dates = pd.Series(dtype='object')

        if self.config.MODO_EXECUCAO == 'local':
            history_df = self._try_load_history()
            if not history_df.empty:
                col_hist_key = schema.COL_ID_STONELAB
                col_hist_date = schema.OUT_COL_ULTIMA_PRESENCA
                
                if col_hist_key not in history_df.columns:
                    if 'id_stonelab' in history_df.columns: col_hist_key = 'id_stonelab'
                    elif 'temp_name_id' in history_df.columns: col_hist_key = 'temp_name_id'
                
                possible_dates = [schema.COL_XML_DATE, 'Date', 'Última Presença', 'Semana', 'date']
                found_date = next((c for c in possible_dates if c in history_df.columns), None)
                if found_date: col_hist_date = found_date

                if col_hist_key in history_df.columns and col_hist_date in history_df.columns:
                    history_df[col_hist_date] = pd.to_datetime(history_df[col_hist_date], errors='coerce')
                    
                    ref_ts = pd.Timestamp(ref_date)
                    history_df = history_df[history_df[col_hist_date] <= ref_ts].copy()

                    history_df['key_norm'] = self._normalize_string(history_df[col_hist_key])
                    hist_by_key = history_df.groupby('key_norm')[col_hist_date].max()
                    
                    df['temp_id_norm'] = self._normalize_string(df[schema.COL_ID_STONELAB])
                    col_nome = schema.COL_NAME if schema.COL_NAME in df.columns else 'Nome'
                    has_name = col_nome in df.columns
                    if has_name:
                        df['temp_name_norm'] = self._normalize_string(df[col_nome])

                    def get_best_date(row):
                        current_date = pd.NaT
                        sid = str(row[schema.COL_ID_STONELAB])
                        if sid in current_last_dates.index:
                            current_date = current_last_dates[sid]
                        if pd.notna(current_date): return current_date

                        hist_date = pd.NaT
                        if row['temp_id_norm'] in hist_by_key.index:
                            hist_date = hist_by_key[row['temp_id_norm']]
                        if pd.isna(hist_date) and has_name and row['temp_name_norm'] in hist_by_key.index:
                            hist_date = hist_by_key[row['temp_name_norm']]
                        return hist_date

                    df[schema.OUT_COL_ULTIMA_PRESENCA] = df.apply(get_best_date, axis=1)
                    df.drop(columns=['temp_id_norm'], inplace=True)
                    if has_name: df.drop(columns=['temp_name_norm'], inplace=True)
                    log.info("Inatividade: Cruzamento Híbrido com histórico concluído.")

        if schema.OUT_COL_ULTIMA_PRESENCA not in df.columns:
            df['sid_str'] = df[schema.COL_ID_STONELAB].astype(str)
            df = df.merge(current_last_dates.rename(schema.OUT_COL_ULTIMA_PRESENCA), 
                          left_on='sid_str', right_index=True, how='left')
        
        return self._finalize_days_calculation(df, ref_date)
    def _finalize_days_calculation(self, df: pd.DataFrame, ref_date: date) -> pd.DataFrame:
        if schema.OUT_COL_ULTIMA_PRESENCA not in df.columns:
            df[schema.OUT_COL_ULTIMA_PRESENCA] = pd.NaT

        def calculate_days(row):
            last_pres = row[schema.OUT_COL_ULTIMA_PRESENCA]
            if pd.isna(last_pres):
                if 'io_start_date' in row and pd.notna(row['io_start_date']):
                    start_date_io = row['io_start_date']
                    if isinstance(start_date_io, pd.Timestamp): start_date_io = start_date_io.date()
                    if start_date_io > ref_date: return 0
                    days_since_start = (ref_date - start_date_io).days
                    return days_since_start if days_since_start >= 0 else 0
                return 999
            if isinstance(last_pres, pd.Timestamp): last_pres = last_pres.date()
            elif isinstance(last_pres, str): last_pres = pd.to_datetime(last_pres).date()
            return (ref_date - last_pres).days

        df[schema.OUT_COL_DIAS_AUSENTE] = df.apply(calculate_days, axis=1)
        if 'io_start_date' in df.columns: df.drop(columns=['io_start_date'], inplace=True)
        return df

    def _classify_and_format(self, df: pd.DataFrame, just_map: Dict[str, Set[date]], 
                             ref_date: date) -> pd.DataFrame:
        conditions = [
            (df[schema.OUT_COL_DIAS_AUSENTE] >= 45),
            (df[schema.OUT_COL_DIAS_AUSENTE] >= 30),
            (df[schema.OUT_COL_DIAS_AUSENTE] >= 15),
            (df[schema.OUT_COL_DIAS_AUSENTE] > 10)
        ]
        choices = [
            schema.RISCO_3_VERMELHO, schema.RISCO_2_LARANJA,
            schema.RISCO_1_AMARELO, schema.RISCO_PRE_INATIVIDADE
        ]
        df[schema.OUT_COL_RISCO] = np.select(conditions, choices, default=schema.RISCO_ATIVO)
        
        def check_justification(row):
            current_status = row[schema.OUT_COL_RISCO]
            if current_status == schema.RISCO_ATIVO: return current_status
            sid = str(row[schema.COL_ID_STONELAB])
            if sid in just_map and ref_date in just_map[sid]: return schema.RISCO_JUSTIFICADO
            return current_status

        df[schema.OUT_COL_RISCO] = df.apply(check_justification, axis=1)
        status_to_hide = [schema.RISCO_ATIVO, schema.RISCO_JUSTIFICADO]
        df_filtered = df[~df[schema.OUT_COL_RISCO].isin(status_to_hide)].copy()
        
        if schema.COL_COORDINATOR in df_filtered.columns:
            df_filtered[schema.OUT_COL_COORDENADOR] = df_filtered[schema.COL_COORDINATOR].apply(
                lambda x: x.name if isinstance(x, Coordinator) else str(x)
            )

        cols_map = {schema.COL_NAME: schema.OUT_COL_NOME, schema.COL_FUNCTION: schema.OUT_COL_FUNCAO}
        df_filtered.rename(columns=cols_map, inplace=True)
        
        final_columns = [
            schema.OUT_COL_NOME, schema.OUT_COL_FUNCAO, schema.OUT_COL_COORDENADOR,
            schema.OUT_COL_ULTIMA_PRESENCA, schema.OUT_COL_DIAS_AUSENTE, schema.OUT_COL_RISCO
        ]
        available_cols = [c for c in final_columns if c in df_filtered.columns]
        df_final = df_filtered[available_cols].copy()
        df_final[schema.OUT_COL_SEMANA] = ref_date
        return df_final.sort_values(by=schema.OUT_COL_DIAS_AUSENTE, ascending=False)