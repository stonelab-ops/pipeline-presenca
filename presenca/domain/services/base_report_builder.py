import pandas as pd
import logging
from datetime import timedelta, date
import schema

log = logging.getLogger(__name__)

class BaseReportBuilder:
    
    def __init__(self, config: object):
        self.config = config

    def build(self, active_students: pd.DataFrame, tenures: dict) -> pd.DataFrame:
        if active_students.empty:
            log.warning("BaseBuilder: Recebi lista de alunos vazia.")
            return pd.DataFrame()

        try:
            report_start = pd.to_datetime(self.config.DATA_INICIO_GERAL).date()
            report_end = pd.to_datetime(self.config.DATA_FIM_GERAL).date()
        except AttributeError:
            log.error("BaseBuilder: Datas de início/fim não configuradas.")
            return pd.DataFrame()

        weeks = []
        
        days_ahead = (0 - report_start.weekday()) % 7 
        if days_ahead == 0:
            current_monday = report_start
        else:
            current_monday = report_start + timedelta(days=days_ahead)

        while current_monday <= report_end:
            weeks.append(current_monday)
            current_monday += timedelta(days=7)

        rows = []
        
        if schema.COL_ID_STONELAB in active_students.columns:
            active_students[schema.COL_ID_STONELAB] = active_students[schema.COL_ID_STONELAB].astype(str).str.strip()

        for student in active_students.itertuples():
            sid = str(getattr(student, schema.COL_ID_STONELAB)).strip()
            
            student_tenures = tenures.get(sid, [])
            
            if not student_tenures:
                continue

            for week_date in weeks:
                freq_esperada = 0
                is_active_this_week = False
                
                for tenure in student_tenures:
                    t_start = tenure.beginning
                    t_end = tenure.end if tenure.end else date.max
                    
                    if t_start <= week_date <= t_end:
                        freq_esperada = tenure.get_expected_frequency(week_date)
                        
                        if freq_esperada > 0:
                            is_active_this_week = True
                            break
                
                if is_active_this_week:
                    row = {
                        schema.COL_ID_STONELAB: sid,
                        schema.COL_NAME: getattr(student, schema.COL_NAME, ""),
                        schema.COL_FUNCTION: getattr(student, schema.COL_FUNCTION, ""),
                        schema.COL_COORDINATOR: getattr(student, schema.COL_COORDINATOR, ""),
                        schema.COL_DATE: week_date,
                        "expected_frequency": int(freq_esperada)
                    }
                    rows.append(row)
            
        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows)