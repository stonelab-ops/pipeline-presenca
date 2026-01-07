PLANILHA_CADASTRO = "[Stone Lab] Cadastro de Pessoal (Responses)"
ABA_CADASTRO_PRINCIPAL = "Form Responses 1"
ABA_NOMES_IGNORAR = "Pessoas para nao monitorar"

PLANILHA_IO_ALUNOS = "IO Alunos no StoneLab (Responses)"
ABA_IO_ALUNOS = "Form Responses 1"

PLANILHA_FERIADOS = "Feriados PUC-Rio"
PLANILHA_JUSTIFICATIVAS = "Aviso de ausência (Responses)"
ABA_JUSTIFICATIVAS = "Form Responses 1"

ARQUIVO_CADASTRO_LOCAL = "cadastro.csv"
ARQUIVO_IO_LOCAL = "io_alunos.csv"
ARQUIVO_IGNORAR_LOCAL = "ignorar.csv"
ARQUIVO_FERIADOS_LOCAL = "feriados.csv"
ARQUIVO_JUSTIFICATIVAS_LOCAL = "justificativas.csv"
PASTA_DASHBOARD_LOCAL = "output-dashboard"
NOME_ARQUIVO_DASHBOARD = "DASHBOARD_SNAPSHOT_ATUAL.xlsx"

CADASTRO_NOME_COMPLETO = "Qual o seu nome completo?"
CADASTRO_FUNCAO = "Qual a sua função no projeto?"
CADASTRO_COORDENADOR = "Quem é o professor responsável pelo seu projeto?"
CADASTRO_ID_STONELAB = "id_stonelab"
CADASTRO_NOME_ENTRADA = "nome_entrada"

IO_COL_ID_RAW = "ID_Stonelab"
IO_COL_START_RAW = "Quando a data de referência?"
IO_COL_FREQ1_RAW = "Frequência esperada"
IO_COL_END1_RAW = "end_date_1"
IO_COL_END2_RAW = "end_date_2"
IO_COL_FREQ2_RAW = "freq_2"

JUSTIFICATIVA_INICIO = "Início da ausência"
JUSTIFICATIVA_FIM = "Fim da ausência"
JUSTIFICATIVA_ID_STONELAB = "Qual seu ID StoneLab? (caso não saiba, procure nossa equipe)"
JUSTIFICATIVA_MOTIVO = "Motivo da ausência"
JUSTIFICATIVA_MOTIVO_FERIAS = "Férias"

FERIADOS_DATA = "Data"

COL_NAME = "name"
COL_FUNCTION = "function"
COL_COORDINATOR = "coordinator"
COL_ID_STONELAB = "id_stonelab"
COL_NOME_ENTRADA = "nome_entrada"
COL_DATE = "date"
COL_XML_DATE = "Date"
COL_START = "start"
COL_END = "end"
COL_REASON = "reason"

COL_IO_START = "start_date"
COL_IO_END1 = "end_date_1"
COL_IO_END2 = "end_date_2"
COL_IO_FREQ1 = "freq_1"
COL_IO_FREQ2 = "freq_2"

OUT_COL_NOME = "Nome"
OUT_COL_FUNCAO = "Função"
OUT_COL_COORDENADOR = "Coordenador"
OUT_COL_SEMANA = "Semana"
OUT_COL_FREQ_OBS = "Freq Obs"
OUT_COL_FREQ_ESP = "Freq Esp"
OUT_COL_DIAS_UTEIS = "Dias Úteis"
OUT_COL_FALTAS_JUST = "Falt Just"
OUT_COL_DIAS_FERIAS = "Dias de Férias"
OUT_COL_SITUACAO = "Situação de Atingimento"

OUT_COL_ACOES_NOME_XML = "nome_entrada"
OUT_COL_ACOES_SEMANA = "Semana"
OUT_COL_ACOES_FREQ_OBS = "Freq Obs"
OUT_COL_ACOES_SITUACAO = "Situacao"

OUT_COL_ULTIMA_PRESENCA = "Última Presença"
OUT_COL_DIAS_AUSENTE = "Dias Ausente"
OUT_COL_RISCO = "Nível de Risco"
OUT_COL_NOME_LIMPEZA = "Nome"
OUT_COL_ULTIMA_PRESENCA_LIMPEZA = "Última Presença"
OUT_COL_DIAS_INATIVO_LIMPEZA = "Dias Ausente"

LIMIAR_ATINGIMENTO_GERAL = 0.75

RISCO_3_VERMELHO = "(3) Vermelho (> 45 dias)"
RISCO_2_LARANJA = "(2) Laranja (> 30 dias)"
RISCO_1_AMARELO = "(1) Amarelo (> 15 dias)"
RISCO_PRE_INATIVIDADE = "(4) Risco Inicial (10-15 dias)"

RISCO_ATIVO = "Ativo"
RISCO_JUSTIFICADO = "Justificado"

STATUS_ATINGIU = "Atingiu"
STATUS_NAO_ATINGIU = "Não Atingiu"
STATUS_JUSTIFICADO = "Semana Justificada"

ACAO_CORRIGIR_NOME = "(1) Corrigir 'nome_entrada' no Cadastro"
ACAO_NAO_CADASTRADO = "(2) Pessoa Não Cadastrada (Verificar)"
ACAO_OK_IGNORADO = "OK (Ignorado)"
ACAO_OK_MATCH_CORRETO = "OK (Match Correto)"

ABA_ACOES_CADASTRO = "Acoes_de_Cadastro"
ABA_RESUMO_POR_ALUNO = "Resumo_por_Aluno"
ABA_REPORT_RAW = "report_raw"
ABA_KPI_GERAL = "kpi_geral_presenca"
ABA_PIVOT_TOTAL = "total_de_alunos_pivot"
ABA_PIVOT_ATINGIDOS = "atingidos_pivot"
ABA_PIVOT_PCT = "pct_atingimento_pivot"
ABA_XML_EXPORT = "export_XMLs"
ABA_RAW_PRESENCE = "raw_presence_data"
ABA_IGNORAR = "Pessoas para nao monitorar"
ABA_INATIVIDADE = "Inatividade_Alunos"
ABA_DB_HISTORICO = "Report_Raw_Historico"
ABA_DB_INATIVIDADE = "Alerta_Inatividade_Historico"
ABA_LIMPEZA_BIOMETRIA = "Limpeza_Biometria_Inativos"

DB_HIST_COL_ID = COL_ID_STONELAB
DB_HIST_COL_NOME = OUT_COL_NOME
DB_HIST_COL_FUNCAO = OUT_COL_FUNCAO
DB_HIST_COL_COORDENADOR = OUT_COL_COORDENADOR
DB_HIST_COL_DATE = OUT_COL_SEMANA
DB_HIST_COL_FREQ_OBS = OUT_COL_FREQ_OBS
DB_HIST_COL_FREQ_ESP = OUT_COL_FREQ_ESP
DB_HIST_COL_DIAS_UTEIS = OUT_COL_DIAS_UTEIS
DB_HIST_COL_FALT_JUST = OUT_COL_FALTAS_JUST
DB_HIST_COL_FERIAS = OUT_COL_DIAS_FERIAS
DB_HIST_COL_SITUACAO = OUT_COL_SITUACAO

COLOR_RED_BG = '#FFC7CE'
COLOR_RED_FONT = '#9C0006'
COLOR_YELLOW_BG = '#FFEB9C'
COLOR_YELLOW_FONT = '#9C6500'
COLOR_ORANGE_BG = '#FCD5B4'
COLOR_ORANGE_FONT = '#9C6500'

FORMAT_RULES_RISCO = {
    "Vermelho": {'bg_color': COLOR_RED_BG, 'font_color': COLOR_RED_FONT},
    "Laranja": {'bg_color': COLOR_ORANGE_BG, 'font_color': COLOR_ORANGE_FONT},
    "Amarelo": {'bg_color': COLOR_YELLOW_BG, 'font_color': COLOR_YELLOW_FONT}
}