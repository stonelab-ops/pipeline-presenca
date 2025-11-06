import pandas as pd
import pytest
from datetime import date

def test_final_report_structure(final_report):
    """Garante que o relatório final não está vazio e tem colunas calculadas."""
    assert isinstance(final_report, pd.DataFrame)
    assert not final_report.empty
    
    expected_cols = [
        'id_stonelab', 'date', 'name', 'function',
        'observed_frequency', 'workdays', 'justified_days', 
        'vacation_days', 'expected_frequency'
    ]
    for col in expected_cols:
        assert col in final_report.columns

def test_workdays_calculation_with_holidays(final_report):
    """
    Verifica se o cálculo de 'workdays' está correto,
    descontando um feriado (15/08/2025).
    """
    week_with_holiday = final_report[final_report['date'] == date(2025, 8, 11)]
    assert not week_with_holiday.empty
    assert week_with_holiday['workdays'].iloc[0] == 4
    
    normal_week = final_report[final_report['date'] == date(2025, 8, 18)]
    assert not normal_week.empty
    assert normal_week['workdays'].iloc[0] == 5

def test_observed_frequency_calculation(final_report):
    """
    Verifica se a 'observed_frequency' foi contada corretamente
    baseado no mock_presenca.xml.
    """

    student_week = final_report[
        (final_report['id_stonelab'] == 'ID_VALIDO_1') & 
        (final_report['date'] == date(2025, 8, 4)) 
    ]

    assert student_week['observed_frequency'].iloc[0] == 1 

    student_week_2 = final_report[
        (final_report['id_stonelab'] == 'ID_VALIDO_1') & 
        (final_report['date'] == date(2025, 8, 11)) 
    ]
    assert student_week_2['observed_frequency'].iloc[0] == 0

def test_expected_frequency_calculation(final_report):
    """
    Verifica se a 'expected_frequency' é lida corretamente do
    mock_io_alunos.csv (ID_VALIDO_1 espera 3 dias).
    """
    student_rows = final_report[final_report['id_stonelab'] == 'ID_VALIDO_1']
    assert (student_rows['expected_frequency'] == 3).all()

def test_justified_days_calculation(final_report):
    """
    Verifica se 'justified_days' foi calculado corretamente
    baseado no mock_justificativas.csv (ID_VALIDO_2 tem 2 dias).
    """
    student_week = final_report[
        (final_report['id_stonelab'] == 'ID_VALIDO_2') & 
        (final_report['date'] == date(2025, 8, 25))
    ]

    assert student_week['justified_days'].iloc[0] == 2