import pytest
import pandas as pd
from presenca.domain.services.report_generators.summary_sheet import SummarySheetGenerator 
from tests import config_test 
from tests.conftest import config_module_to_dict

@pytest.fixture(scope="module") 
def summary_generator(report_with_kpis): 
    """Instancia o SummarySheetGenerator com o relatório final calculado."""
    config_dict = config_module_to_dict(config_test)
    
    if report_with_kpis.empty:
        pytest.fail("A fixture 'report_with_kpis' está vazia.")
        
    return SummarySheetGenerator(report_kpi=report_with_kpis, config=config_dict)

def test_generate_summary_calculates_correctly(summary_generator):
    tabs = summary_generator.generate()
    assert isinstance(tabs, dict)
    assert "Resumo_por_Aluno" in tabs
    df_resumo = tabs["Resumo_por_Aluno"]
    assert isinstance(df_resumo, pd.DataFrame)
    assert not df_resumo.empty, "O DataFrame de resumo não deveria estar vazio."
    
    expected_cols = ['Nome do Aluno', 'Coordenador', 'Situacao Geral no Mês']
    for col in expected_cols:
        assert col in df_resumo.columns

    resumo_aluno1 = df_resumo[df_resumo['Nome do Aluno'] == 'Aluno Ativo Simples'].iloc[0]
    assert resumo_aluno1['Situacao Geral no Mês'] == 'Não Atingiu Meta Geral'

    resumo_aluno2 = df_resumo[df_resumo['Nome do Aluno'] == 'Aluno Jornada Complexa'].iloc[0]
    assert resumo_aluno2['Situacao Geral no Mês'] == 'Não Atingiu Meta Geral'

def test_generate_summary_handles_empty_input(summary_generator):
    empty_report = pd.DataFrame(columns=summary_generator.report_kpi.columns)
    config_dict = summary_generator.config 
    empty_generator = SummarySheetGenerator(report_kpi=empty_report, config=config_dict)
    tabs = empty_generator.generate()
    
    assert isinstance(tabs, dict)
    assert "Resumo_por_Aluno" in tabs
    df_resumo = tabs["Resumo_por_Aluno"]
    assert isinstance(df_resumo, pd.DataFrame)
    assert df_resumo.empty

def test_generate_summary_handles_all_justified_weeks(summary_generator):
    report_all_justified = summary_generator.report_kpi.copy()
    report_all_justified['Situação de Atingimento'] = 'Semana Justificada'
    config_dict = summary_generator.config 
    justified_generator = SummarySheetGenerator(report_kpi=report_all_justified, config=config_dict)
    tabs = justified_generator.generate()
    
    assert isinstance(tabs, dict)
    assert "Resumo_por_Aluno" in tabs
    df_resumo = tabs["Resumo_por_Aluno"]
    assert isinstance(df_resumo, pd.DataFrame)
    assert df_resumo.empty