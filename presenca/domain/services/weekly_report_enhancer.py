import pandas as pd
from datetime import date, timedelta
from typing import Dict, Set

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
        """Orquestra a adição de todas as colunas de enriquecimento."""
        holidays = self._prepare_holidays(holidays_df)
        justifications = self._prepare_justifications(justifications_df)

        report = pd.merge(base_report, student_info, on='id_stonelab', how='left')
        report = self._add_observed_frequency(report, attendance)
        
        report = self._add_derived_weekly_columns(
            report, holidays, justifications, tenures
        )
        return report

    def _prepare_holidays(self, df: pd.DataFrame) -> Set[date]:
        """Converte o DataFrame de feriados em um set de datas."""
        if df.empty or 'Data' not in df.columns:
            return set()
        
        dates = pd.to_datetime(df['Data'], format="%d/%m/%Y", errors='coerce')
        return set(dates.dropna().dt.date)

    def _normalize_justification_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renomeia e padroniza as colunas do DataFrame de justificativas."""
        df_copy = df.copy()
        df_copy.columns = df_copy.columns.str.strip()
        col_map = {
            "Início da ausência": "start",
            "Fim da ausência": "end",
            "Qual seu ID StoneLab? ...": "id_stonelab", 
            "Motivo da ausência": "reason"
        }
        df_copy.rename(columns={k: v for k, v in col_map.items() if k in df_copy.columns}, inplace=True)
        return df_copy

    def _prepare_justifications(self, df_just_raw: pd.DataFrame) -> Dict[str, Dict[str, Set]]:
        """Converte o DataFrame de justificativas em um dicionário aninhado."""
        if df_just_raw.empty: 
            return {}
            
        df_just = df_just_raw.copy()
        df_just.columns = df_just.columns.str.strip()
        
        col_map = {
            "Início da ausência": "start", 
            "Fim da ausência": "end", 
            "Qual seu ID StoneLab? (caso não saiba, procure nossa equipe)": "id_stonelab", 
            "Motivo da ausência": "reason"
        }
        
        df_just.rename(columns=col_map, inplace=True)
        
        required_cols = ['start', 'end', 'id_stonelab']
        if not all(col in df_just.columns for col in required_cols): 
            return {}
            
        df_just['start'] = pd.to_datetime(df_just['start'], errors='coerce').dt.date
        df_just['end'] = pd.to_datetime(df_just['end'], errors='coerce').dt.date
        
        justified_days = {}
        valid_rows = df_just.dropna(subset=['start', 'end', 'id_stonelab'])
        for row in valid_rows.itertuples(index=False):
            sid = str(row.id_stonelab)
            
            if sid not in justified_days: 
                justified_days[sid] = {'total': set(), 'vacation': set()}
            
            try: 
                if row.start <= row.end:
                    dates = set(pd.date_range(start=row.start, end=row.end).date)
                    justified_days[sid]['total'].update(dates)
                    if hasattr(row, 'reason') and row.reason == "Férias": 
                        justified_days[sid]['vacation'].update(dates)
            except Exception as e: 
                pass 
                
        return justified_days
    
    def _add_iso_date_columns(self, df: pd.DataFrame, date_col: str) -> pd.DataFrame:
        """Adiciona colunas de 'week' e 'year' baseadas em uma coluna de data."""
        df_copy = df.copy()
        iso_dates = pd.to_datetime(df_copy[date_col]).dt.isocalendar()
        df_copy['week'] = iso_dates.week
        df_copy['year'] = iso_dates.year
        return df_copy

    def _add_observed_frequency(self, report: pd.DataFrame, attendance: pd.DataFrame) -> pd.DataFrame:
        """Calcula e adiciona a frequência observada por semana."""
        if attendance.empty:
            report['observed_frequency'] = 0
            return report
        
        report_iso = self._add_iso_date_columns(report, 'date')
        attendance_iso = self._add_iso_date_columns(attendance, 'Date')
        
        group_cols = ['id_stonelab', 'year', 'week']
        freq = attendance_iso.groupby(group_cols).size()
        freq = freq.reset_index(name='observed_frequency')
        
        report = pd.merge(report_iso, freq, on=group_cols, how='left')
        report['observed_frequency'] = report['observed_frequency'].fillna(0).astype(int)
        return report

    def _add_derived_weekly_columns(
        self, report: pd.DataFrame, holidays: set, justifications: dict, tenures: dict
    ) -> pd.DataFrame:
        """Adiciona colunas calculadas de dias úteis, justificativas, etc."""
        report['workdays'] = report['date'].apply(
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
        """Calcula os dias úteis em uma semana, descontando feriados."""
        week_days = {start_date + timedelta(days=i) for i in range(5)}
        return 5 - len(week_days.intersection(holidays))

    def _calculate_justified_days(self, row: pd.Series, r_type: str, just: dict) -> int:
        """Calcula dias justificados em uma semana para um aluno."""
        sid = str(row['id_stonelab'])
        if sid not in just:
            return 0
        week_days = {row['date'] + timedelta(days=i) for i in range(7)}
        return len(week_days.intersection(just[sid][r_type]))
    
    def _calculate_expected_frequency(self, row: pd.Series, tenures: dict) -> int:
        """Obtém a frequência esperada de um aluno para uma data."""
        sid = str(row['id_stonelab'])
        if sid in tenures:
            for t in tenures[sid]:
                if t.active_at_date(row['date']):
                    return t.get_expected_frequency(row['date'])
        return 0