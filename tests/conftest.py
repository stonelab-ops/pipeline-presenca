# tests/conftest.py

import pytest
import pandas as pd
from datetime import date # Mantenha esta importação

from presenca.utils.data_reader import DataReader
from presenca.domain.services.data_processing import DataProcessingService
from tests import config_test
from presenca.domain.services.base_report_builder import BaseReportBuilder
from presenca.domain.services.weekly_report_enhancer import WeeklyReportEnhancer
from presenca.domain.services.kpi_calculator import KpiCalculator 

def config_module_to_dict(config_module):
    config_dict = {
        key: getattr(config_module, key)
        for key in dir(config_module)
        if not key.startswith("__")
    }
    config_dict.setdefault('INICIO_REPORT', "2025-08-01") 
    config_dict.setdefault('FIM_REPORT', "2025-09-30") 
    config_dict.setdefault('LIMIAR_ATINGIMENTO_GERAL', 0.75)
    return config_dict

@pytest.fixture(scope="session")
def processed_data():
    """Lê e processa os dados mock."""
    print("\n(GLOBAL) Lendo e processando dados mock uma única vez: ")
    reader = DataReader(config=config_test)
    raw_data = reader.load_all_sources()
    processor = DataProcessingService(raw_data, config_test)
    return processor.run()

@pytest.fixture(scope="session")
def base_report(processed_data):
    """Cria o esqueleto do relatório."""
    print("\n(GLOBAL) Executando o BaseReportBuilder...")
    df_depara = processed_data['cadastro']
    tenures_dict = processed_data['tenures']
    active_student_ids = tenures_dict.keys()
    active_students = df_depara[
        df_depara['id_stonelab'].isin(active_student_ids)
    ].copy()
    builder = BaseReportBuilder(config=config_test)
    return builder.build(active_students=active_students, tenures=tenures_dict)

@pytest.fixture(scope="session")
def final_report(base_report, processed_data):
    """Cria o relatório final SEM a coluna de KPI."""
    print("\n(GLOBAL) Executando o WeeklyReportEnhancer...")
    enhancer = WeeklyReportEnhancer()
    report = enhancer.enhance( 
        base_report=base_report,
        attendance=processed_data['registros_final'],
        student_info=processed_data['cadastro'],
        holidays_df=processed_data['feriados'],
        justifications_df=processed_data['justificativas'],
        tenures=processed_data['tenures']
    )
    return report 

@pytest.fixture(scope="session")
def report_with_kpis(final_report, processed_data):
    """
    Executa o KpiCalculator para obter o relatório COM a coluna 
    'Situação de Atingimento'.
    """
    print("\n(GLOBAL) Executando KpiCalculator...")
    config_dict = config_module_to_dict(config_test)
    calculator = KpiCalculator(
        report=final_report, 
        data_frames=processed_data, 
        config=config_dict
    )
    calculated_report = calculator.calculate()
    assert 'Situação de Atingimento' in calculated_report.columns, \
        "KpiCalculator não adicionou a coluna 'Situação de Atingimento'"
    
    return calculated_report