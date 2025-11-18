import pandas as pd
from ....utils import schema
from ...models.coordinator import Coordinator

class SummarySheetGenerator:
    def __init__(self, report_kpi: pd.DataFrame, config: dict):
        self.report_kpi = report_kpi
        self.config = config

    def generate(self) -> dict:
        if self.report_kpi.empty or schema.OUT_COL_SITUACAO not in self.report_kpi.columns:
            return {schema.ABA_RESUMO_POR_ALUNO: pd.DataFrame()}
            
        df = self.report_kpi[
            self.report_kpi[schema.OUT_COL_SITUACAO] != schema.STATUS_JUSTIFICADO
        ].copy()
        
        if df.empty:
            return {schema.ABA_RESUMO_POR_ALUNO: pd.DataFrame()}
            
        df["Atingimento_Bin"] = df[schema.OUT_COL_SITUACAO].map(
            lambda x: 1 if x == schema.STATUS_ATINGIU else 0
        )
        
        df['coordinator_name'] = df[schema.COL_COORDINATOR].apply(
            lambda x: x.name if isinstance(x, Coordinator) else str(x)
        )
        
        resumo = df.groupby([schema.COL_NAME, 'coordinator_name']).agg(
            semanas_contabilizadas=(schema.COL_DATE, 'count'),
            semanas_atingidas=('Atingimento_Bin', 'sum')
        ).reset_index()
        
        if resumo.empty:
            return {schema.ABA_RESUMO_POR_ALUNO: pd.DataFrame()}
            
        resumo['pct'] = (
            resumo['semanas_atingidas'] / resumo['semanas_contabilizadas']
        )
        
        limiar = self.config.LIMIAR_ATINGIMENTO_GERAL
        
        resumo['Status'] = resumo['pct'].apply(
            lambda x: schema.STATUS_ATINGIU if x >= limiar else schema.STATUS_NAO_ATINGIU
        )
        
        resumo.rename(
            columns={
                schema.COL_NAME: 'Nome do Aluno', 
                'coordinator_name': 'Coordenador',
                'Status': 'Situacao Geral no Mês'
            },
            inplace=True
        )
        cols = ['Nome do Aluno', 'Coordenador', 'Situacao Geral no Mês']
        return {schema.ABA_RESUMO_POR_ALUNO: resumo[cols]}