import pandas as pd
import pytest

def test_base_report_is_not_empty(base_report):
    assert isinstance(base_report, pd.DataFrame)
    assert not base_report.empty, "O DataFrame base não deveria estar vazio."

def test_base_report_has_correct_columns(base_report):
    expected_columns = ['id_stonelab', 'date']
    for col in expected_columns:
        assert col in base_report.columns

def test_base_report_student_active_weeks(base_report):
    """
    Verifica se o 'ID_VALIDO_1' (que não tem data de fim no mock)
    é gerado até o final do período de teste.
    """
    student_rows = base_report[base_report['id_stonelab'] == 'ID_VALIDO_1']

    assert len(student_rows) == 74, "ID_VALIDO_1 deveria ter 74 semanas ativas (até o fim do config)."
