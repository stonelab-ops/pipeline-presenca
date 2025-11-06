import pytest
from tests import config_test
from presenca.domain.services.kpi_calculator import KpiCalculator

def config_module_to_dict(config_module):
    """Converte o módulo de config em um dict para a classe KpiCalculator."""
    config_dict = {
        key: getattr(config_module, key)
        for key in dir(config_module)
        if not key.startswith("__")
    }
    
    config_dict['INICIO_REPORT'] = "2025-08-01" 
    config_dict['FIM_REPORT'] = "2025-09-30" 
        
    return config_dict

def test_kpi_calculator_calcula_status_corretamente(final_report, processed_data):
    """
    Testa se o KpiCalculator aplica os status de atingimento corretamente.
    """
    config_dict = config_module_to_dict(config_test)
    
    calculator = KpiCalculator(
        report=final_report, 
        data_frames=processed_data, 
        config=config_dict
    )
    
    report_with_kpi = calculator.calculate()

    semana_1 = report_with_kpi[
        (report_with_kpi['id_stonelab'] == 'ID_VALIDO_1') &
        (report_with_kpi['date'].astype(str) == '2025-08-04')
    ].iloc[0]
    assert semana_1['Situação de Atingimento'] == 'Não Atingiu'
    
    semana_2 = report_with_kpi[
        (report_with_kpi['id_stonelab'] == 'ID_VALIDO_2') &
        (report_with_kpi['date'].astype(str) == '2025-08-25')
    ].iloc[0]
    assert semana_2['Situação de Atingimento'] == 'Não Atingiu'
    
    semana_3 = report_with_kpi[
        (report_with_kpi['id_stonelab'] == 'ID_VALIDO_2') &
        (report_with_kpi['date'].astype(str) == '2025-09-01')
    ].iloc[0]
    assert semana_3['Situação de Atingimento'] == 'Semana Justificada'