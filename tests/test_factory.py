from datetime import date

def test_aluno_com_saida(processed_data):
    print("\n(teste) Aluno com data de saída:")
    all_tenures = processed_data['tenures']
    assert 'ID_VALIDO_2' in all_tenures
    tenure = all_tenures['ID_VALIDO_2'][0]
    assert tenure.end == date(2025, 9, 30)