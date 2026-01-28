import sys
import pytest
import pandas as pd
from pathlib import Path
import os
import shutil
import logging
import importlib

TEST_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = TEST_DIR.parent
sys.path.append(str(PROJECT_ROOT))

FIXTURES_DIR = TEST_DIR / "fixtures"

TEMP_INPUT_DIR = TEST_DIR / "temp_input_mock"
MOCK_OUTPUT_DIR = TEST_DIR / "output_mock"
MOCK_DASHBOARD_DIR = MOCK_OUTPUT_DIR / "dashboard_mock"

try:
    import configs.settings_local as settings
except ImportError:
    pytest.fail("Arquivo configs/settings_local.py nao encontrado")

import main

def test_pipeline_com_fixtures_mock(caplog):
    caplog.set_level(logging.INFO)
    print(f"\n[SETUP] Preparando ambiente Sandbox (Isolado)...")

    if TEMP_INPUT_DIR.exists(): shutil.rmtree(TEMP_INPUT_DIR)
    if MOCK_OUTPUT_DIR.exists(): shutil.rmtree(MOCK_OUTPUT_DIR)
    
    TEMP_INPUT_DIR.mkdir(parents=True)
    MOCK_OUTPUT_DIR.mkdir(parents=True)

    if not FIXTURES_DIR.exists():
        pytest.fail(f"❌ Pasta 'fixtures' não encontrada em: {FIXTURES_DIR}")

    arquivos_copiados = 0
    for file_path in FIXTURES_DIR.glob("*"):
        if file_path.suffix.lower() in ['.xml', '.csv', '.xlsx']:
            shutil.copy(file_path, TEMP_INPUT_DIR)
            arquivos_copiados += 1
            print(f"   -> Copiado: {file_path.name}")
    
    if arquivos_copiados == 0:
        pytest.fail("❌ A pasta 'tests/fixtures' está vazia ou sem arquivos válidos (xml/csv)!")

    input_orig = settings.CAMINHOS['local']['dados_presenca']
    output_orig = settings.CAMINHOS['local']['output']
    dashboard_orig = settings.CAMINHOS['local']['dashboard']
    ano_orig = settings.ANO_DO_RELATORIO
    mes_orig = settings.MES_DO_RELATORIO

    settings.CAMINHOS['local']['dados_presenca'] = str(TEMP_INPUT_DIR)
    settings.CAMINHOS['local']['output'] = str(MOCK_OUTPUT_DIR)
    settings.CAMINHOS['local']['dashboard'] = str(MOCK_DASHBOARD_DIR)

    settings.ANO_DO_RELATORIO = 2025
    settings.MES_DO_RELATORIO = 11

    try:
        print(f"▶️ Executando Pipeline (Simulando {settings.MES_DO_RELATORIO}/{settings.ANO_DO_RELATORIO})...")
        
        if hasattr(main, 'run_pipeline'):
            main.run_pipeline()
        else:
            exec(open(PROJECT_ROOT / "main.py").read())
            
    except Exception as e:
        print("\n❌ ERRO CRÍTICO NO PIPELINE:")
        for record in caplog.records:
            print(f"   [{record.levelname}] {record.message}")
        pytest.fail(f"O código quebrou durante a execução: {e}")

    arquivos_gerados = list(MOCK_OUTPUT_DIR.glob("*.xlsx"))
    
    if not arquivos_gerados:
        print("\n❌ FALHA: Pipeline finalizou sem erros, mas SEM gerar Excel.")
        print("🔍 Dica: Verifique se a data do XML de mock bate com 11/2025.")
        pytest.fail("Nenhum arquivo Excel encontrado na saída.")

    arquivo_excel = arquivos_gerados[0]
    print(f"[SUCESSO] Arquivo gerado: {arquivo_excel.name}")

    try:
        xls = pd.ExcelFile(arquivo_excel)
        abas = xls.sheet_names
        
        if "report_raw" not in abas:
            pytest.fail(f"Aba 'report_raw' não encontrada. Abas disponíveis: {abas}")
            
        df = pd.read_excel(xls, "report_raw")
        if df.empty:
             print("⚠️ Aviso: A aba 'report_raw' foi gerada mas está vazia (sem presenças calculadas).")
        else:
             print(f"[SUCESSO] Relatório contém {len(df)} linhas de dados.")

    except Exception as e:
        pytest.fail(f"Erro ao ler o Excel gerado: {e}")

    finally:
        print("\n[TEARDOWN] Restaurando configurações e limpando lixo...")
        try:
            if TEMP_INPUT_DIR.exists(): shutil.rmtree(TEMP_INPUT_DIR)
            if MOCK_OUTPUT_DIR.exists(): shutil.rmtree(MOCK_OUTPUT_DIR)
        except: pass

        settings.CAMINHOS['local']['dados_presenca'] = input_orig
        settings.CAMINHOS['local']['output'] = output_orig
        settings.CAMINHOS['local']['dashboard'] = dashboard_orig
        settings.ANO_DO_RELATORIO = ano_orig
        settings.MES_DO_RELATORIO = mes_orig

if __name__ == "__main__":
    pytest.main([__file__])