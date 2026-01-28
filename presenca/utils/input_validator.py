import logging
import schema

log = logging.getLogger(__name__)

def validar_estrutura_inputs(dados_brutos):
    """
    Analisa os DataFrames carregados e verifica se possuem as colunas obrigatórias
    definidas no schema.py.
    """
    print("\n" + "="*60)
    print("🧐 [CHECK] INICIANDO VALIDAÇÃO DOS DADOS DE ENTRADA")
    print("="*60)

    erros_encontrados = []

    # --- 1. Validação do CADASTRO ---
    df_cadastro = dados_brutos.get('cadastro')
    if df_cadastro is None or df_cadastro.empty:
        erros_encontrados.append("❌ ERRO CRÍTICO: Planilha de 'Cadastro' não foi carregada ou está vazia.")
    else:
        cols_esperadas = schema.COLUNAS_OBRIGATORIAS_CADASTRO
        cols_atuais = df_cadastro.columns.tolist()
        colunas_faltantes = [col for col in cols_esperadas if col not in cols_atuais]
        if colunas_faltantes:
            erros_encontrados.append(f"❌ ERRO NO CADASTRO: Faltam as colunas obrigatórias: {colunas_faltantes}")
        else:
            print("✅ [OK] Cadastro: Estrutura válida.")

    # --- 2. Validação do IO (Entradas e Saídas) ---
    df_io = dados_brutos.get('io_alunos')
    if df_io is None or df_io.empty:
        erros_encontrados.append("❌ ERRO CRÍTICO: Planilha de 'IO Alunos' não foi carregada ou está vazia.")
    else:
        cols_esperadas_io = [schema.COL_NOME_CADASTRO, "Carimbo de data/hora", "Tipo de Registro"]
        print(f"✅ [OK] IO Alunos: {len(df_io)} registros encontrados.")

    df_feriados = dados_brutos.get('feriados')
    if df_feriados is not None and not df_feriados.empty:
        print(f"✅ [OK] Feriados: Tabela carregada com sucesso.")
    else:
        print("⚠️ [AVISO] Feriados: Tabela vazia ou não carregada (O sistema rodará sem descontar feriados).")

    # --- CONCLUSÃO ---
    print("-" * 60)
    if erros_encontrados:
        print("🚨 VALIDAÇÃO FALHOU! CORRIJA OS ERROS ABAIXO PARA CONTINUAR:")
        for erro in erros_encontrados:
            print(f"   -> {erro}")
        print("="*60 + "\n")
        return False 
    else:
        print("🚀 SUCESSO: Todos os inputs estão saudáveis. Iniciando Pipeline...")
        print("="*60 + "\n")
        return True 