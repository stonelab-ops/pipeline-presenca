import os

PLANILHA_CADASTRO = "[Stone Lab] Cadastro de Pessoal (Responses)"
ABA_CADASTRO_PRINCIPAL = "Form Responses 1"
ABA_NOMES_IGNORAR = "Pessoas para nao monitorar"
PLANILHA_IO_ALUNOS = "IO Alunos no StoneLab (Responses)"
ABA_IO_ALUNOS = "Form Responses 1"
PLANILHA_FERIADOS = "Feriados PUC-Rio"
PLANILHA_JUSTIFICATIVAS = "Aviso de ausência (Responses)"
ABA_JUSTIFICATIVAS = "Form Responses 1"
LIMIAR_ATINGIMENTO_GERAL = 0.75

CAMINHOS = {
    'colab': {
        'dados_presenca': "/gdrive/MyDrive/projetos-colab-compartilhados/Sistema_Gestao_Presenca/Database/Raw_unstructured_data/xml_biometria",
        'id_pasta_saida': "15MPcIJyU5xsf-vMDvw2Sa7a2KJZjoObt",
        'dashboard': 'output-dashboard',
        'test_data': 'test_data'
    }
}

MODO_EXECUCAO = 'colab'