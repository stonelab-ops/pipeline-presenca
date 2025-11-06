import pandas as pd

class SummarySheetGenerator:
    def __init__(self, report_kpi: pd.DataFrame, config: dict):
        self.report_kpi = report_kpi
        self.config = config

    def generate(self) -> dict:
        if self.report_kpi.empty:
            return {"Resumo_por_Aluno": pd.DataFrame()}
            
        df = self.report_kpi[
            self.report_kpi['Situação de Atingimento'] != "Semana Justificada"
        ].copy()
        
        df["Atingimento_Bin"] = df["Situação de Atingimento"].map(
            lambda x: 1 if x == "Atingiu" else 0
        )
        
        resumo = df.groupby(['name', 'coordinator']).agg(
            semanas_contabilizadas=('date', 'count'),
            semanas_atingidas=('Atingimento_Bin', 'sum')
        ).reset_index()
        
        if resumo.empty:
            return {"Resumo_por_Aluno": pd.DataFrame()}
            
        resumo['pct'] = (
            resumo['semanas_atingidas'] / resumo['semanas_contabilizadas']
        )
        
        limiar = self.config.LIMIAR_ATINGIMENTO_GERAL
        
        resumo['Status'] = resumo['pct'].apply(
            lambda x: 'Atingiu Meta Geral' if x >= limiar else 'Não Atingiu Meta Geral'
        )
        
        resumo.rename(
            columns={'name': 'Nome do Aluno', 'coordinator': 'Coordenador',
                     'Status': 'Situacao Geral no Mês'},
            inplace=True
        )
        cols = ['Nome do Aluno', 'Coordenador', 'Situacao Geral no Mês']
        return {"Resumo_por_Aluno": resumo[cols]}