import sys
import pytest
import pandas as pd
from pathlib import Path

TEST_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = TEST_DIR.parent
sys.path.append(str(PROJECT_ROOT))

import schema

DATA_PATH = PROJECT_ROOT / "test_data"

def test_caminho_dados_existe():
    assert DATA_PATH.exists(), f"Diretório não encontrado: {DATA_PATH}"

def test_arquivos_fundamentais_existem():
    arquivos = ["cadastro.csv", "io_alunos.csv", "feriados.csv"]
    for arquivo in arquivos:
        assert (DATA_PATH / arquivo).exists(), f"Arquivo faltando: {arquivo}"

def test_validar_cadastro_estrutura_e_dados():
    path = DATA_PATH / "cadastro.csv"
    try:
        df = pd.read_csv(path, sep=None, engine='python', encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(path, sep=None, engine='python', encoding='latin1')
    
    # colunas
    missing = set(schema.COLUNAS_OBRIGATORIAS_CADASTRO) - set(df.columns)
    assert not missing, f"Colunas faltando no Cadastro: {missing}"
    
    # Nulos
    nulos = df[schema.COL_ID_STONELAB].isnull().sum()
    assert nulos == 0, f"Existem {nulos} IDs vazios no cadastro."

    # Duplos 
    duplicados = df[schema.COL_ID_STONELAB].duplicated().sum()
    assert duplicados == 0, f"Existem {duplicados} IDs duplicados no cadastro."

def test_validar_io_catraca():
    path = DATA_PATH / "io_alunos.csv"
    try:
        try:
            df = pd.read_csv(path, sep=None, engine='python', encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(path, sep=None, engine='python', encoding='latin1')
    except pd.errors.EmptyDataError:
        pytest.fail("O arquivo io_alunos.csv está vazio.")

    missing = set(schema.COLUNAS_OBRIGATORIAS_IO) - set(df.columns)
    assert not missing, f"Colunas faltando no IO: {missing}"

    assert len(df) > 0, "O arquivo de IO existe mas não possui registros."

if __name__ == "__main__":
    pytest.main([__file__])