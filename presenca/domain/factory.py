import pandas as pd
import logging
from typing import Dict, List, Optional
from .models.tenure import Tenure, FrequencyChange
from .models.coordinator import Coordinator
from ..utils import schema

log = logging.getLogger(__name__)

class TenureFactory:
    
    def create_tenures_from_df(self, df: pd.DataFrame) -> Dict[str, List[Tenure]]:
        tenures_dict = {}
        df_cleaned = self._clean_io_df(df)
        
        if df_cleaned.empty:
            return {}

        if schema.COL_IO_START in df_cleaned.columns and schema.COL_ID_STONELAB in df_cleaned.columns:
            df_cleaned.sort_values(by=schema.COL_IO_START, ascending=False, inplace=True, na_position='last')
            
            original_len = len(df_cleaned)
            df_cleaned.drop_duplicates(subset=[schema.COL_ID_STONELAB], keep='first', inplace=True)
            
            if len(df_cleaned) < original_len:
                log.info(f"Factory: Deduplicação aplicada. Removidos {original_len - len(df_cleaned)} registros antigos de IO.")

        for row in df_cleaned.itertuples():
            sid_raw = getattr(row, schema.COL_ID_STONELAB, None)
            if pd.isna(sid_raw):
                continue
                
            sid = str(sid_raw).strip().replace('.0', '')
            
            if sid not in tenures_dict:
                tenures_dict[sid] = []
            
            tenure = self._create_tenure_from_row(row)
            if tenure:
                tenures_dict[sid].append(tenure)
                
        return tenures_dict

    def _clean_io_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df_copy = df.copy()
        df_copy.columns = df_copy.columns.str.strip()
        
        rename_map = {
            schema.IO_COL_ID_RAW: schema.COL_ID_STONELAB,
            schema.IO_COL_START_RAW: schema.COL_IO_START,
            schema.IO_COL_END1_RAW: schema.COL_IO_END1,
            schema.IO_COL_END2_RAW: schema.COL_IO_END2,
            schema.IO_COL_FREQ1_RAW: schema.COL_IO_FREQ1,
            schema.IO_COL_FREQ2_RAW: schema.COL_IO_FREQ2
        }
        df_copy.rename(columns=rename_map, inplace=True)

        if schema.COL_ID_STONELAB not in df_copy.columns:
            log.warning("Factory: Coluna ID exata não encontrada. Tentando busca inteligente...")
            found_col = None
            for col in df_copy.columns:
                c_lower = col.lower()
                if "id" in c_lower and "stonelab" in c_lower:
                    found_col = col
                    break
            
            if found_col:
                log.info(f"Factory: Coluna ID localizada como '{found_col}'. Renomeando.")
                df_copy.rename(columns={found_col: schema.COL_ID_STONELAB}, inplace=True)
            else:
                return pd.DataFrame()

        if schema.COL_IO_FREQ1 not in df_copy.columns:
            found_freq = None
            for col in df_copy.columns:
                c_lower = col.lower()
                if "freq" in c_lower and "2" not in c_lower:
                    found_freq = col
                    break
            
            if found_freq:
                log.info(f"Factory: Coluna Freq localizada como '{found_freq}'. Renomeando.")
                df_copy.rename(columns={found_freq: schema.COL_IO_FREQ1}, inplace=True)

        if schema.COL_IO_FREQ1 in df_copy.columns:
            mask_saiu = df_copy[schema.COL_IO_FREQ1].astype(str).str.contains("Saiu", case=False, na=False)
            
            if mask_saiu.any():
                count_saiu = mask_saiu.sum()
                log.info(f"Factory: Detectados {count_saiu} registros com 'Saiu' na coluna de Frequência. Zerando.")
                df_copy.loc[mask_saiu, schema.COL_IO_FREQ1] = 0

        date_cols = [schema.COL_IO_START, schema.COL_IO_END1, schema.COL_IO_END2]
        for col in date_cols:
            if col in df_copy.columns:
                df_copy[col] = pd.to_datetime(df_copy[col], dayfirst=True, errors='coerce').dt.date
            else:
                df_copy[col] = pd.NaT
        
        if schema.COL_ID_STONELAB in df_copy.columns and schema.COL_IO_START in df_copy.columns:
            return df_copy.dropna(subset=[schema.COL_ID_STONELAB, schema.COL_IO_START])
        else:
            return pd.DataFrame()

    def _create_tenure_from_row(self, row: pd.Series) -> Optional[Tenure]:
        try:
            start = getattr(row, schema.COL_IO_START)
            if pd.isna(start):
                return None

            end1 = getattr(row, schema.COL_IO_END1) if hasattr(row, schema.COL_IO_END1) else None
            end2 = getattr(row, schema.COL_IO_END2) if hasattr(row, schema.COL_IO_END2) else None
            freq1 = getattr(row, schema.COL_IO_FREQ1) if hasattr(row, schema.COL_IO_FREQ1) else 0
            freq2 = getattr(row, schema.COL_IO_FREQ2) if hasattr(row, schema.COL_IO_FREQ2) else 0

            def safe_int(val):
                try:
                    if pd.isna(val): return 0
                    if isinstance(val, int): return val
                    if isinstance(val, float): return int(val)
                    if isinstance(val, str) and val.strip().isdigit(): return int(val.strip())
                    return 0
                except:
                    return 0

            f1 = safe_int(freq1)
            f2 = safe_int(freq2)
            
            final_end = end1 if pd.notna(end1) else None
            changes = []

            if f2 > 0 and pd.notna(end1):
                changes.append(FrequencyChange(reference_date=end1, new_expected_frequency=f2))
                final_end = end2 if pd.notna(end2) else None

            return Tenure(
                beginning=start,
                original_expected_frequency=f1,
                end=final_end,
                frequency_changes=changes
            )
        except Exception:
            return None

class CoordinatorFactory:
    
    def __init__(self):
        self._coordinators: Dict[str, Coordinator] = {}

    def get_or_create(self, raw_name: str) -> Coordinator:
        if not isinstance(raw_name, str) or pd.isna(raw_name) or raw_name.strip() == "":
            raw_name = "Indefinido"
            
        normalized_name = raw_name.strip().title()
        
        if normalized_name not in self._coordinators:
            self._coordinators[normalized_name] = Coordinator(normalized_name)
            
        return self._coordinators[normalized_name]