import pandas as pd
import numpy as np
import logging
from datetime import date
from typing import Dict, Set, Any, List
import schema
from ...models.coordinator import Coordinator
from .inactivity_calculator import InactivityCalculator

log = logging.getLogger(__name__)

class InactivitySheetGenerator:
    
    def __init__(self, processed_data: dict, config: dict):
        self.processed_data = processed_data
        self.cadastro = processed_data.get('cadastro', pd.DataFrame())
        self.tenures = processed_data.get('tenures', {})
        self.justificativas_raw = processed_data.get('justificativas', pd.DataFrame())
        self.config = config
        self.calculator = InactivityCalculator(processed_data, config)

    def generate(self) -> dict:
        log.info("Gerador Inatividade: Iniciando análise...")
        
        try:
            ref_date = pd.to_datetime(self.config.DATA_FIM_GERAL).date()
            start_month = pd.to_datetime(self.config.DATA_INICIO_GERAL).date()
        except Exception:
            return {schema.ABA_INATIVIDADE: pd.DataFrame()}
        
        active_ids = self._get_active_students_ids(start_month, ref_date)
        if not active_ids:
            return {schema.ABA_INATIVIDADE: pd.DataFrame()}

        justifications_map = self._prepare_justifications_map()
        df_risk = self._prepare_and_deduplicate_students(active_ids)
        df_risk = self.calculator.calculate_last_presence(df_risk, ref_date)
        df_final = self._classify_and_format(df_risk, justifications_map, ref_date)
        return {schema.ABA_INATIVIDADE: df_final}

    def _prepare_and_deduplicate_students(self, active_ids: List[str]) -> pd.DataFrame:
        df = self.cadastro[
            self.cadastro[schema.COL_ID_STONELAB].astype(str).isin(active_ids)
        ].copy()

        col_ativo = getattr(schema, 'CADASTRO_ATIVO', 'Ativo')
        if col_ativo in df.columns:
            df['ativo_temp'] = pd.to_numeric(df[col_ativo], errors='coerce').fillna(0)
            df = df[df['ativo_temp'] == 1].copy()
            df.drop(columns=['ativo_temp'], inplace=True)

        df['io_start_date'] = df[schema.COL_ID_STONELAB].astype(str).map(self._get_start_dates_map())

        df.sort_values(by='io_start_date', ascending=False, inplace=True, na_position='last')

        col_nome = schema.COL_NAME if schema.COL_NAME in df.columns else 'Nome'
        
        if col_nome in df.columns:
            df['temp_dedup_name'] = df[col_nome].astype(str).str.upper().str.strip()
            df.drop_duplicates(subset=['temp_dedup_name'], keep='first', inplace=True)
            df.drop(columns=['temp_dedup_name'], inplace=True)
            
        return df

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
        candidates = ['frequency','frequency_1','freq','freq_1','expected_frequency','original_expected_frequency']
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
                start_map[str(sid)] = max(valid_starts)
        return start_map

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
        
        if 'io_start_date' in df_final.columns:
             df_final.drop(columns=['io_start_date'], inplace=True)

        return df_final.sort_values(by=schema.OUT_COL_DIAS_AUSENTE, ascending=False)