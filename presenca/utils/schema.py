# 1. Nomes das Colunas de ENTRADA 
# 'cadastro.csv'
CADASTRO_NOME_COMPLETO = "Qual o seu nome completo?"
CADASTRO_FUNCAO = "Qual a sua função no projeto?"
CADASTRO_COORDENADOR = "Quem é o professor responsável pelo seu projeto?"
CADASTRO_ID_STONELAB = "id_stonelab"
CADASTRO_NOME_ENTRADA = "nome_entrada" 

# 'justificativas.csv'
JUSTIFICATIVA_INICIO = "Início da ausência"
JUSTIFICATIVA_FIM = "Fim da ausência"
JUSTIFICATIVA_ID_STONELAB = "Qual seu ID StoneLab? (caso não saiba, procure nossa equipe)"
JUSTIFICATIVA_MOTIVO = "Motivo da ausência"
JUSTIFICATIVA_MOTIVO_FERIAS = "Férias"

# 'feriados.csv'
FERIADOS_DATA = "Data"

# 2. Nomes das Colunas INTERNAS (pipeline) 
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


# 3. Nomes das Colunas de SAÍDA (Excel) 
# 'report_raw'
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

# 'Acoes_de_Cadastro'
OUT_COL_ACOES_NOME_XML = "nome_entrada"
OUT_COL_ACOES_SEMANA = "Semana"
OUT_COL_ACOES_FREQ_OBS = "Freq Obs"
OUT_COL_ACOES_SITUACAO = "Situacao"


# 4. Valores de Status (Resultados dos Cálculos) 
STATUS_ATINGIU = "Atingiu"
STATUS_NAO_ATINGIU = "Não Atingiu"
STATUS_JUSTIFICADO = "Semana Justificada"

# 'Acoes_de_Cadastro'
ACAO_CORRIGIR_NOME = "(1) Corrigir 'nome_entrada' no Cadastro"
ACAO_NAO_CADASTRADO = "(2) Pessoa Não Cadastrada (Verificar)"
ACAO_OK_IGNORADO = "OK (Ignorado)"
ACAO_OK_MATCH_CORRETO = "OK (Match Correto)"


# 5. Nomes das Abas (Tabs) Finais do Excel 
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