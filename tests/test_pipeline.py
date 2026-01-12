import sys
import pytest
import pandas as pd
from pathlib import Path
import os

TEST_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = TEST_DIR.parent
sys.path.append(str(PROJECT_ROOT))

try:
    import configs.settings_local as settings
except ImportError:
    pytest.fail("Arquivo configs/settings_local.py nao encontrado")

import main

def test_pipeline_na_vida_real():
    raw_path_output = settings.CAMINHOS['local'].get('output')
    if raw_path_output:
        caminho_saida = Path(raw_path_output)
    else:
        caminho_saida = PROJECT_ROOT / "output"

    if not caminho_saida.exists():
        caminho_saida.mkdir(parents=True)

    print(f"\n[TESTE] Monitorando saida em: {caminho_saida}")

    ano = settings.ANO_DO_RELATORIO
    mes = settings.MES_DO_RELATORIO
    padrao_mes = f"*{ano}-{mes:02d}*.xlsx"

    for arq in list(caminho_saida.glob(padrao_mes)):
        try:
            os.remove(arq)
        except:
            pass

    arquivos_antes = set(caminho_saida.glob("*.xlsx"))

    try:
        if hasattr(main, 'run_pipeline'):
            main.run_pipeline()
        else:
            exec(open(PROJECT_ROOT / "main.py").read())
    except Exception as e:
        pytest.fail(f"Erro na execucao do Pipeline: {e}")

    arquivos_depois = set(caminho_saida.glob("*.xlsx"))
    novos_arquivos = arquivos_depois - arquivos_antes

    assert len(novos_arquivos) > 0, f"Nenhum Excel novo foi gerado em: {caminho_saida}"

    arquivo_gerado = novos_arquivos.pop()
    print(f"[SUCESSO] Arquivo gerado: {arquivo_gerado.name}")

    try:
        todas_abas = pd.read_excel(arquivo_gerado, sheet_name=None)
        
        EXPECTATIVAS = {
            "report_raw": [
                "id_stonelab", 
                "Nome", 
                "Freq Obs", 
                "Situação de Atingimento"
            ],
            "Acoes_de_Cadastro": [      
                "nome_entrada", 
                "Situacao"
            ]
        }

        for aba_esperada, colunas_esperadas in EXPECTATIVAS.items():
            assert aba_esperada in todas_abas, f"A aba '{aba_esperada}' sumiu do Excel! Encontrei: {list(todas_abas.keys())}"
            
            df = todas_abas[aba_esperada]
            assert len(df) > 0, f"A aba '{aba_esperada}' foi gerada mas esta vazia!"

            for col in colunas_esperadas:
                assert col in df.columns, f"Na aba '{aba_esperada}', falta a coluna: {col}"

        print(f"[SUCESSO] Todas as {len(EXPECTATIVAS)} abas foram validadas com sucesso.")

    except Exception as e:
        pytest.fail(f"Erro na auditoria do Excel: {e}")

    finally:
        try:
            if arquivo_gerado.exists():
                os.remove(arquivo_gerado)
        except:
            pass

if __name__ == "__main__":
    pytest.main([__file__])