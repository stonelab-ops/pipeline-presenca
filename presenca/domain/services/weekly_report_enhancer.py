import pandas as pd
from datetime import date, timedelta
from typing import Dict, Set
from ...utils import schema 

class WeeklyReportEnhancer:

    def enhance(
        self,
        base_report: pd.DataFrame,
        attendance: pd.DataFrame,
        student_info: pd.DataFrame,
        holidays_df: pd.DataFrame,
        justifications_df: pd.DataFrame,
        tenures: Dict
    ) -> pd.DataFrame:
        holidays = self._prepare_holidays(holidays_df)
        justifications = self._prepare_justifications(justifications_df)

        report = pd.merge(base_report, student_info, on=schema.COL_ID_STONELAB, how='left')
        report = self._add_observed_frequency(report, attendance)
        
        report = self._add_derived_weekly_columns(
            report, holidays, justifications, tenures
        )
        return report

    def _prepare_holidays(self, df: pd.DataFrame) -> Set[date]:
        if df.empty or schema.FERIADOS_DATA not in df.columns:
            return set()
        
        dates = pd.to_datetime(df[schema.FERIADOS_DATA], format="%d/%m/%Y", errors='coerce')
        return set(dates.dropna().dt.date)

    def _normalize_justification_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df_copy = df.copy()
        df_copy.columns = df_copy.columns.str.strip()
        col_map = {
            "Início da ausência": schema.COL_START,
            "Fim da ausência": schema.COL_END,
            "Qual seu ID StoneLab? ...": schema.COL_ID_STONELAB, 
            "Motivo da ausência": schema.COL_REASON
        }
        df_copy.rename(columns={k: v for k, v in col_map.items() if k in df_copy.columns}, inplace=True)
        return df_copy

    def _prepare_justifications(self, df_just_raw: pd.DataFrame) -> Dict[str, Dict[str, Set]]:
        if df_just_raw.empty: 
            return {}
            
        df_just = df_just_raw.copy()
        df_just.columns = df_just.columns.str.strip()
        
        col_map = {
            schema.JUSTIFICATIVA_INICIO: schema.COL_START, 
            schema.JUSTIFICATIVA_FIM: schema.COL_END, 
            schema.JUSTIFICATIVA_ID_STONELAB: schema.COL_ID_STONELAB, 
            schema.JUSTIFICATIVA_MOTIVO: schema.COL_REASON
        }
        
        df_just.rename(columns=col_map, inplace=True)
        
        required_cols = [schema.COL_START, schema.COL_END, schema.COL_ID_STONELAB]
        if not all(col in df_just.columns for col in required_cols): 
            return {}
            
        df_just[schema.COL_START] = pd.to_datetime(df_just[schema.COL_START], errors='coerce').dt.date
        df_just[schema.COL_END] = pd.to_datetime(df_just[schema.COL_END], errors='coerce').dt.date
        
        justified_days = {}
        valid_rows = df_just.dropna(subset=required_cols)
        for row in valid_rows.itertuples(index=False):
            sid = str(row.id_stonelab)
            
            if sid not in justified_days: 
                justified_days[sid] = {'total': set(), 'vacation': set()}
            
            try: 
                if row.start <= row.end:
                    dates = set(pd.date_range(start=row.start, end=row.end).date)
                    justified_days[sid]['total'].update(dates)
                    if hasattr(row, schema.COL_REASON) and row.reason == schema.JUSTIFICATIVA_MOTIVO_FERIAS: 
                        justified_days[sid]['vacation'].update(dates)
            except Exception as e: 
                pass 
                
        return justified_days
    
    def _add_iso_date_columns(self, df: pd.DataFrame, date_col: str) -> pd.DataFrame:
        df_copy = df.copy()
        iso_dates = pd.to_datetime(df_copy[date_col]).dt.isocalendar()
        df_copy['week'] = iso_dates.week
        df_copy['year'] = iso_dates.year
        return df_copy

    def _add_observed_frequency(self, report: pd.DataFrame, attendance: pd.DataFrame) -> pd.DataFrame:
        if attendance.empty:
            report['observed_frequency'] = 0
            return report
        
        report_iso = self._add_iso_date_columns(report, schema.COL_DATE)
        attendance_iso = self._add_iso_date_columns(attendance, schema.COL_XML_DATE)
        
        group_cols = [schema.COL_ID_STONELAB, 'year', 'week']
        freq = attendance_iso.groupby(group_cols).size()
        freq = freq.reset_index(name='observed_frequency')
        
        report = pd.merge(report_iso, freq, on=group_cols, how='left')
        report['observed_frequency'] = report['observed_frequency'].fillna(0).astype(int)
        return report

    def _add_derived_weekly_columns(
        self, report: pd.DataFrame, holidays: set, justifications: dict, tenures: dict
    ) -> pd.DataFrame:
        report['workdays'] = report[schema.COL_DATE].apply(
            lambda d: self._calculate_workdays(d, holidays)
        )
        report['justified_days'] = report.apply(
            lambda r: self._calculate_justified_days(r, 'total', justifications),
            axis=1
        )
        report['vacation_days'] = report.apply(
            lambda r: self._calculate_justified_days(r, 'vacation', justifications),
            axis=1
        )
        report['expected_frequency'] = report.apply(
            lambda r: self._calculate_expected_frequency(r, tenures), axis=1
        )
        return report

    def _calculate_workdays(self, start_date: date, holidays: Set) -> int:
        week_days = {start_date + timedelta(days=i) for i in range(5)}
        return 5 - len(week_days.intersection(holidays))

    def _calculate_justified_days(self, row: pd.Series, r_type: str, just: dict) -> int:
        sid = str(row[schema.COL_ID_STONELAB])
        if sid not in just:
            return 0
        week_days = {row[schema.COL_DATE] + timedelta(days=i) for i in range(7)}
        return len(week_days.intersection(just[sid][r_type]))
    
    def _calculate_expected_frequency(self, row: pd.Series, tenures: dict) -> int:
        sid = str(row[schema.COL_ID_STONELAB])
        if sid in tenures:
            for t in tenures[sid]:
                if t.active_at_date(row[schema.COL_DATE]):
                    return t.get_expected_frequency(row[schema.COL_DATE])
        return 0