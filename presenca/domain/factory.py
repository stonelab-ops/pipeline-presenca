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
        
        if schema.COL_IO_START in df_cleaned.columns and schema.COL_ID_STONELAB in df_cleaned.columns:
            df_cleaned.sort_values(by=schema.COL_IO_START, ascending=False, inplace=True, na_position='last')
            
            original_len = len(df_cleaned)
            df_cleaned.drop_duplicates(subset=[schema.COL_ID_STONELAB], keep='first', inplace=True)
            
            if len(df_cleaned) < original_len:
                log.info(f"Factory: Deduplicação aplicada. Removidos {original_len - len(df_cleaned)} registros antigos de IO.")

        for row in df_cleaned.itertuples():
            sid = str(getattr(row, schema.COL_ID_STONELAB))
            if sid not in tenures_dict:
                tenures_dict[sid] = []
            
            tenure = self._create_tenure_from_row(row)
            if tenure:
                tenures_dict[sid].append(tenure)
                
        return tenures_dict

    def _clean_io_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df_copy = df.copy()
        df_copy.columns = df_copy.columns.str.strip()
        
        df_copy.rename(columns={
            schema.IO_COL_ID_RAW: schema.COL_ID_STONELAB,
            schema.IO_COL_START_RAW: schema.COL_IO_START,
            schema.IO_COL_END1_RAW: schema.COL_IO_END1,
            schema.IO_COL_END2_RAW: schema.COL_IO_END2,
            schema.IO_COL_FREQ1_RAW: schema.COL_IO_FREQ1,
            schema.IO_COL_FREQ2_RAW: schema.COL_IO_FREQ2
        }, inplace=True)

        date_cols = [schema.COL_IO_START, schema.COL_IO_END1, schema.COL_IO_END2]
        
        for col in date_cols:
            if col in df_copy.columns:
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce').dt.date
            else:
                df_copy[col] = pd.NaT
        
        return df_copy.dropna(subset=[schema.COL_ID_STONELAB, schema.COL_IO_START])

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