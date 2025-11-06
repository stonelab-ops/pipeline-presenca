import pytest
import pandas as pd
from datetime import date
from presenca.domain.services.report_generators.kpi_sheets import KpiSheetGenerator

@pytest.fixture(scope="module")
def kpi_generator(report_with_kpis): 
    """Instancia o KpiSheetGenerator com o relatório final calculado."""
    if report_with_kpis.empty:
        pytest.fail("A fixture global 'report_with_kpis' está vazia.")
    return KpiSheetGenerator(report_kpi=report_with_kpis) 


def test_create_report_raw_renaming_and_columns(kpi_generator):
    df_raw = kpi_generator._create_report_raw()
    assert isinstance(df_raw, pd.DataFrame)
    assert not df_raw.empty
    expected_columns = [
        "Nome", "Coordenador", "Semana", "Freq Obs", "Freq Esp",
        "Dias Úteis", "Falt Just", "Dias de Férias", "Situação de Atingimento"
    ]
    for col in expected_columns:
        assert col in df_raw.columns 
    assert 'name' not in df_raw.columns
    assert 'date' not in df_raw.columns
    assert 'observed_frequency' not in df_raw.columns

def test_calculate_base_metrics_filters_justified_weeks(kpi_generator):
    """
    Este teste agora usará a fixture global correta.
    A linha '2025-09-01' (Semana Justificada) será filtrada.
    """
    gpb = kpi_generator._calculate_base_metrics()
    week_sep_01 = date(2025, 9, 1)
    filtered_out = gpb[gpb['date'] == week_sep_01]
    assert filtered_out.empty 

def test_calculate_base_metrics_calculates_correctly(kpi_generator):
    gpb = kpi_generator._calculate_base_metrics()
    assert isinstance(gpb, pd.DataFrame)
    assert not gpb.empty
    assert 'total_de_alunos' in gpb.columns
    assert 'atingidos' in gpb.columns
    assert 'pct_atingimento' in gpb.columns

    week_aug_04 = gpb[(gpb['date'] == date(2025, 8, 4)) & (gpb['coordinator'] == 'Coordenador Alfa')]
    assert not week_aug_04.empty
    assert week_aug_04.iloc[0]['total_de_alunos'] == 2
    assert week_aug_04.iloc[0]['atingidos'] == 0
    assert week_aug_04.iloc[0]['pct_atingimento'] == 0.0

    week_aug_11 = gpb[(gpb['date'] == date(2025, 8, 11)) & (gpb['coordinator'] == 'Coordenador Alfa')]
    assert not week_aug_11.empty
    assert week_aug_11.iloc[0]['total_de_alunos'] == 1
    assert week_aug_11.iloc[0]['atingidos'] == 0
    assert week_aug_11.iloc[0]['pct_atingimento'] == 0.0

def test_create_pivot_tabs_structure(kpi_generator):
    gpb = kpi_generator._calculate_base_metrics()
    pivots = kpi_generator._create_pivot_tabs(gpb)
    assert isinstance(pivots, dict)
    assert 'total_de_alunos_pivot' in pivots
    assert 'atingidos_pivot' in pivots
    assert 'pct_atingimento_pivot' in pivots

    df_pivot = pivots['pct_atingimento_pivot']
    assert isinstance(df_pivot, pd.DataFrame)
    assert not df_pivot.empty
    assert 'coordinator' in df_pivot.columns 

    date_columns = [col for col in df_pivot.columns if col != 'coordinator']
    assert len(date_columns) > 0 
    try:
        pd.to_datetime(date_columns[0], format='%Y-%m-%d')
    except ValueError:
        pytest.fail(f"Coluna de data no pivot não está no formato YYYY-MM-DD: {date_columns[0]}")

def test_create_kpi_geral_tab_calculates_correctly(kpi_generator):
    gpb = kpi_generator._calculate_base_metrics()
    df_kpi_geral = kpi_generator._create_kpi_geral_tab(gpb)
    assert isinstance(df_kpi_geral, pd.DataFrame)
    assert not df_kpi_geral.empty
    assert 'date' in df_kpi_geral.columns
    assert 'total_de_alunos' in df_kpi_geral.columns
    assert 'atingidos' in df_kpi_geral.columns
    assert 'KPI_Presenca' in df_kpi_geral.columns

    week_aug_04 = df_kpi_geral[df_kpi_geral['date'] == date(2025, 8, 4)].iloc[0]
    assert week_aug_04['total_de_alunos'] == 2 
    assert week_aug_04['atingidos'] == 0
    assert week_aug_04['KPI_Presenca'] == 0.0

def test_generate_method_returns_all_tabs(kpi_generator):
    all_tabs = kpi_generator.generate()
    assert isinstance(all_tabs, dict)
    expected_keys = [
        'report_raw', 
        'total_de_alunos_pivot', 
        'atingidos_pivot', 
        'pct_atingimento_pivot', 
        'kpi_geral_presenca'
    ]
    for key in expected_keys:
        assert key in all_tabs
        assert isinstance(all_tabs[key], pd.DataFrame)
        assert not all_tabs[key].empty