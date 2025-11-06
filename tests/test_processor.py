from IPython.display import display

def test_processador_filtra_dados_corretamente(processed_data):
    print("\n (TESTE) Processamento Inicial de Dados:")
    df_resultado = processed_data['registros_final']
    
    assert len(df_resultado) == 2, f"Esperado 2 registros, mas foram encontrados {len(df_resultado)}."
    
    nomes_finais = df_resultado['nome_entrada'].unique()
    assert 'aluno_fantasma' not in nomes_finais
    
    print("\nVerificações passaram! Amostra de 'registros_final':")
    display(df_resultado.head())