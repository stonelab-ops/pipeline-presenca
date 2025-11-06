import pytest
import pandas as pd
import os
from presenca.utils.data_reader import DataReader
from tests import config_test

@pytest.fixture
def data_reader_local():
    return DataReader(config=config_test)

def test_leitura_dados_locais(data_reader_local):
    print("\n(teste) Leitura de Dados Locais (Mock):")
    
    reader = data_reader_local
    dados = reader.load_all_sources()
    df_cadastro = dados['cadastro']
    df_presenca = dados['registros_brutos']

    assert len(df_cadastro) == 4, f"Esperado 4 registros do mock, mas foram lidos {len(df_cadastro)}."
    assert len(df_presenca) == 6, f"Esperado 6 registros do mock, mas foram lidos {len(df_presenca)}."
    print("\nVerificações automáticas passaram.")

    df_presenca.rename(columns={'Name': 'nome_entrada'}, inplace=True)
    df_cadastro.columns = df_cadastro.columns.str.strip()
    df_merged = pd.merge(df_presenca, df_cadastro, on='nome_entrada', how='left')

    print("\nVisualização Final do Teste de leitura: ")
    print(df_merged[['nome_entrada', 'Qual o seu nome completo?', 'id_stonelab']].to_string())