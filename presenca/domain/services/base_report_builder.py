import pandas as pd
from typing import Dict, List

class BaseReportBuilder:
    """
    Constrói a estrutura base (scaffold) do relatório semanal.
    """
    def __init__(self, config: object):
        self.config = config

    def build(
        self, active_students: pd.DataFrame, tenures: Dict
    ) -> pd.DataFrame:
        """
        Gera o DataFrame base a partir dos alunos ativos e suas jornadas.
        """
        report_weeks = self._get_report_weeks_range()
        
        all_rows = []
        for student in active_students.itertuples(index=False):
            student_rows = self._generate_rows_for_student(
                student, report_weeks, tenures
            )
            all_rows.extend(student_rows)

        if not all_rows:
            return pd.DataFrame(columns=['id_stonelab', 'date'])
        return pd.DataFrame(all_rows)

    def _get_report_weeks_range(self) -> pd.DatetimeIndex:
        """Retorna o range de datas (início de semana) para o relatório."""
        return pd.date_range(
            start=self.config.DATA_INICIO_GERAL,
            end=self.config.DATA_FIM_GERAL,
            freq='W-MON'
        ).date

    def _generate_rows_for_student(
        self, student: object, weeks: pd.DatetimeIndex, tenures: Dict
    ) -> List[Dict]:
        """Cria as linhas de relatório para um único aluno."""
        student_id = str(student.id_stonelab)
        student_rows = []
        if student_id in tenures:
            for week_date in weeks:
                if any(t.active_at_date(week_date) for t in tenures[student_id]):
                    student_rows.append({
                        'id_stonelab': student.id_stonelab,
                        'date': week_date
                    })
        return student_rows