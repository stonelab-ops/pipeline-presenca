import pandas as pd
import numpy as np
import schema
from ...models.coordinator import Coordinator

class SummarySheetGenerator:
    def __init__(self, report_kpi: pd.DataFrame, config: dict):
        self.report_kpi = report_kpi
        self.config = config

    def generate(self) -> dict:
        if self.report_kpi.empty or 'meta_dinamica' not in self.report_kpi.columns:
            return {schema.ABA_RESUMO_POR_ALUNO: pd.DataFrame()}
            
        df = self.report_kpi.copy()
        
        df['coordinator_name'] = df[schema.COL_COORDINATOR].apply(
            lambda x: x.name if isinstance(x, Coordinator) else str(x)
        )
        
        resumo = df.groupby([schema.COL_NAME, 'coordinator_name']).agg(
            total_presenca=('observed_frequency', 'sum'),
            total_meta=('meta_dinamica', 'sum'),
            total_justificativas=('justified_days', 'sum') 
        ).reset_index()
        
        if resumo.empty:
            return {schema.ABA_RESUMO_POR_ALUNO: pd.DataFrame()}

        def calcular_porcentagem(row):
            if row['total_meta'] == 0:
                return 1.0 
            return row['total_presenca'] / row['total_meta']

        resumo['ratio_atingimento'] = resumo.apply(calcular_porcentagem, axis=1)

        def definir_status(row):
            if row['total_presenca'] >= row['total_meta']:
                return schema.STATUS_ATINGIU
            if row['total_justificativas'] > 0:
                return schema.STATUS_JUSTIFICADO
            return schema.STATUS_NAO_ATINGIU

        resumo['Status'] = resumo.apply(definir_status, axis=1)
        resumo['Atingimento %'] = (resumo['ratio_atingimento'] * 100
                                   ).fillna(0).round(0).astype(int).astype(str) + '%'

        resumo.rename(
            columns={
                schema.COL_NAME: 'Nome do Aluno', 
                'coordinator_name': 'Coordenador',
                'Status': 'Situacao Geral no Mês'
            },
            inplace=True
        )
        
        cols = ['Nome do Aluno', 'Coordenador', 'Situacao Geral no Mês', 'Atingimento %']
        
        return {schema.ABA_RESUMO_POR_ALUNO: resumo[cols]}