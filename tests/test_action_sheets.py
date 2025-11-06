import pytest
import pandas as pd
from datetime import date
from presenca.domain.services.report_generators.action_sheets import ActionSheetGenerator
from tests import config_test
from presenca.utils.date_utils import get_workdays_for_week 

@pytest.fixture(scope="module")
def action_generator(processed_data):
    config_dict = {
        key: getattr(config_test, key)
        for key in dir(config_test) if not key.startswith("__")
    }
    config_dict.setdefault('INICIO_REPORT', '2025-08-01')
    config_dict.setdefault('FIM_REPORT', '2025-08-31')

    required_keys = ['registros_brutos', 'cadastro', 'ignorar_final', 'ignorar', 'feriados']
    if not all(key in processed_data for key in required_keys):
        pytest.fail(f"A fixture 'processed_data' não contém todas as chaves esperadas: {required_keys}")

    return ActionSheetGenerator(data_frames=processed_data, config=config_dict)

def test_classify_unmatched_name_scenario_3(action_generator):
    nome_xml = "Aluno Sem Nome Entrada"
    set_nomes_completos = {"Aluno Sem Nome Entrada", "Aluno Ativo Simples"}
    acao, situacao = action_generator._classify_unmatched_name(
        nome_xml, set_nomes_completos
    )
    assert "(3)" in situacao
    assert "CORRIGIR" in acao

def test_classify_unmatched_name_scenario_4(action_generator):
    nome_xml = "Nome Totalmente Novo"
    set_nomes_completos = {"Aluno Ativo Simples", "Aluno Jornada Complexa"}
    acao, situacao = action_generator._classify_unmatched_name(
        nome_xml, set_nomes_completos
    )
    assert "(4)" in situacao
    assert "VERIFICAR" in acao

def test_create_action_list_df_identifies_unmatched(action_generator):
    start_date = pd.to_datetime(action_generator.config['INICIO_REPORT']).date()
    end_date = pd.to_datetime(action_generator.config['FIM_REPORT']).date()
    df_acoes = action_generator._create_action_list_df(start_date, end_date)

    assert isinstance(df_acoes, pd.DataFrame)
    assert not df_acoes.empty

    aluno_fantasma_row = df_acoes[df_acoes['Nome no XML'] == 'aluno_fantasma']
    assert not aluno_fantasma_row.empty
    assert "(4)" in aluno_fantasma_row.iloc[0]['Situacao']
    assert "VERIFICAR" in aluno_fantasma_row.iloc[0]['Acao Sugerida']

    assert 'aluno_ativo_1' not in df_acoes['Nome no XML'].tolist()

def test_create_details_unmatched_tab(action_generator):
    start_date = pd.to_datetime(action_generator.config['INICIO_REPORT']).date()
    end_date = pd.to_datetime(action_generator.config['FIM_REPORT']).date()
    df_acoes = action_generator._create_action_list_df(start_date, end_date) 
    df_details = action_generator._create_details_unmatched_tab(df_acoes, start_date, end_date)

    assert isinstance(df_details, pd.DataFrame)
    assert not df_details.empty
    
    fantasma_details = df_details[df_details['Nome'] == 'aluno_fantasma']
    assert not fantasma_details.empty
    
    fantasma_week = fantasma_details[fantasma_details['Semana'] == date(2025, 8, 4)]
    assert not fantasma_week.empty
    
    assert fantasma_week.iloc[0]['Freq Obs'] == 1
    assert fantasma_week.iloc[0]['Dias Úteis'] == 5 
    assert fantasma_week.iloc[0]['Freq Esp'] == 0
    assert fantasma_week.iloc[0]['Coordenador'] == '(Não Classificado)'
    assert "(4)" in fantasma_week.iloc[0]['Situacao'] 

def test_create_details_unmatched_tab_empty_actions(action_generator):
    start_date = pd.to_datetime(action_generator.config['INICIO_REPORT']).date()
    end_date = pd.to_datetime(action_generator.config['FIM_REPORT']).date()
    empty_actions = pd.DataFrame(columns=['Nome no XML', 'Situacao', 'Acao Sugerida'])
    df_details = action_generator._create_details_unmatched_tab(empty_actions, start_date, end_date)
    assert isinstance(df_details, pd.DataFrame)
    assert df_details.empty

def test_generate_raw_data_tabs(action_generator):
    start_date = pd.to_datetime(action_generator.config['INICIO_REPORT']).date()
    end_date = pd.to_datetime(action_generator.config['FIM_REPORT']).date()
    raw_tabs = action_generator._generate_raw_data_tabs(start_date, end_date)

    assert isinstance(raw_tabs, dict)
    assert 'export_XMLs' in raw_tabs
    assert 'raw_presence_data' in raw_tabs
    assert 'Pessoas para nao monitorar' in raw_tabs

    df_xml = raw_tabs['export_XMLs']
    df_presence = raw_tabs['raw_presence_data']
    df_ignorar = raw_tabs['Pessoas para nao monitorar']

    assert isinstance(df_xml, pd.DataFrame)
    assert isinstance(df_presence, pd.DataFrame)
    assert isinstance(df_ignorar, pd.DataFrame)

    assert not df_xml.empty
    assert not df_presence.empty
    assert not df_ignorar.empty 
    
    assert 'Nome (XML)' in df_xml.columns
    assert 'Data (XML)' in df_xml.columns
    assert df_xml['Data (XML)'].min() >= start_date
    assert df_xml['Data (XML)'].max() <= end_date

    assert df_presence['Date'].min() >= start_date
    assert df_presence['Date'].max() <= end_date

    assert 'pessoa_a_ignorar' in df_ignorar['Nomes a Ignorar'].tolist()

def test_generate_method_returns_all_tabs(action_generator):
    all_tabs = action_generator.generate()

    assert isinstance(all_tabs, dict)
    expected_keys = [
        'Acoes_de_Cadastro', 
        'Detalhes_Sem_Match', 
        'export_XMLs', 
        'raw_presence_data', 
        'Pessoas para nao monitorar'
    ]
    for key in expected_keys:
        assert key in all_tabs
        assert isinstance(all_tabs[key], pd.DataFrame)

    assert not all_tabs['Acoes_de_Cadastro'].empty
    assert not all_tabs['Detalhes_Sem_Match'].empty
    assert not all_tabs['export_XMLs'].empty
    assert not all_tabs['raw_presence_data'].empty
    assert not all_tabs['Pessoas para nao monitorar'].empty