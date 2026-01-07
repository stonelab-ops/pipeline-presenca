import pandas as pd
import numpy as np
import schema
from ..models.tenure import Tenure

class WeeklyReportEnhancer:
    
    def enhance(self, base_report: pd.DataFrame, attendance: pd.DataFrame, 
                student_info: pd.DataFrame, holidays_df: pd.DataFrame, 
                justifications_df: pd.DataFrame, tenures: dict) -> pd.DataFrame:
        
        report = base_report.copy()
        
        if schema.COL_ID_STONELAB in report.columns:
            report[schema.COL_ID_STONELAB] = report[schema.COL_ID_STONELAB].astype(str).str.strip()
            
        if schema.COL_ID_STONELAB in attendance.columns:
            attendance[schema.COL_ID_STONELAB] = attendance[schema.COL_ID_STONELAB].astype(str).str.strip()

        report = self._add_observed_frequency(report, attendance)
        report = self._add_workdays_and_holidays(report, holidays_df)
        report = self._add_justifications(report, justifications_df)
        
        return report

    def _add_observed_frequency(self, report: pd.DataFrame, attendance: pd.DataFrame) -> pd.DataFrame:
        if attendance.empty:
            report['observed_frequency'] = 0
            return report

        if 'Datetime' in attendance.columns:
            attendance.rename(columns={'Datetime': schema.COL_DATE}, inplace=True)
        elif 'Date' in attendance.columns:
            attendance.rename(columns={'Date': schema.COL_DATE}, inplace=True)

        if schema.COL_DATE not in attendance.columns:
            report['observed_frequency'] = 0
            return report

        attendance[schema.COL_DATE] = pd.to_datetime(attendance[schema.COL_DATE], errors='coerce')
        report[schema.COL_DATE] = pd.to_datetime(report[schema.COL_DATE], errors='coerce')

        week_map = report[[schema.COL_DATE]].drop_duplicates().sort_values(schema.COL_DATE)
        week_map['week_end'] = week_map[schema.COL_DATE] + pd.Timedelta(days=6)
        
        def get_week_start(date_val):
            for _, row in week_map.iterrows():
                if row[schema.COL_DATE] <= date_val <= row['week_end']:
                    return row[schema.COL_DATE]
            return pd.NaT

        attendance['week_start'] = attendance[schema.COL_DATE].apply(get_week_start)
        
        freq_final = attendance.groupby([schema.COL_ID_STONELAB, 'week_start']).size().reset_index(name='observed_frequency')
        
        report = pd.merge(
            report, 
            freq_final, 
            left_on=[schema.COL_ID_STONELAB, schema.COL_DATE],
            right_on=[schema.COL_ID_STONELAB, 'week_start'],
            how='left'
        )
        
        report['observed_frequency'] = report['observed_frequency'].fillna(0).astype(int)
        
        if 'week_start' in report.columns:
            report.drop(columns=['week_start'], inplace=True)
            
        return report

    def _add_workdays_and_holidays(self, report: pd.DataFrame, holidays_df: pd.DataFrame) -> pd.DataFrame:
        holidays = set()
        if not holidays_df.empty and schema.FERIADOS_DATA in holidays_df.columns:
            holidays = set(pd.to_datetime(holidays_df[schema.FERIADOS_DATA], dayfirst=True, errors='coerce').dt.date)

        def calculate_days(row):
            start = row[schema.COL_DATE].date()
            week_days = pd.date_range(start, periods=5, freq='B') 
            
            holidays_in_week = 0
            for day in week_days:
                if day.date() in holidays:
                    holidays_in_week += 1
            
            return 5 - holidays_in_week, holidays_in_week

        results = report.apply(calculate_days, axis=1, result_type='expand')
        report['workdays'] = results[0]
        report['vacation_days'] = results[1] 
        
        return report

    def _add_justifications(self, report: pd.DataFrame, just_df: pd.DataFrame) -> pd.DataFrame:
        report['justified_days'] = 0
        
        if just_df.empty:
            return report

        just_df = just_df.copy()
        if schema.COL_ID_STONELAB in just_df.columns:
             just_df[schema.COL_ID_STONELAB] = just_df[schema.COL_ID_STONELAB].astype(str).str.strip()
        
        just_map = {}
        col_start = schema.JUSTIFICATIVA_INICIO if schema.JUSTIFICATIVA_INICIO in just_df.columns else schema.COL_START
        col_end = schema.JUSTIFICATIVA_FIM if schema.JUSTIFICATIVA_FIM in just_df.columns else schema.COL_END
        col_id = schema.JUSTIFICATIVA_ID_STONELAB if schema.JUSTIFICATIVA_ID_STONELAB in just_df.columns else schema.COL_ID_STONELAB

        for row in just_df.itertuples():
            try:
                sid = str(getattr(row, col_id)).strip()
                s_date = pd.to_datetime(getattr(row, col_start), dayfirst=True, errors='coerce')
                e_date = pd.to_datetime(getattr(row, col_end), dayfirst=True, errors='coerce')
                
                if pd.isna(s_date) or pd.isna(e_date): continue
                
                if sid not in just_map: just_map[sid] = set()
                just_map[sid].update(pd.date_range(s_date, e_date).date)
            except: continue

        def count_justified(row):
            sid = str(row[schema.COL_ID_STONELAB]).strip()
            if sid not in just_map: return 0
            
            week_start = row[schema.COL_DATE].date()
            week_days = pd.date_range(week_start, periods=5, freq='B').date
            
            count = 0
            for day in week_days:
                if day in just_map[sid]:
                    count += 1
            return count

        report['justified_days'] = report.apply(count_justified, axis=1)
        return report