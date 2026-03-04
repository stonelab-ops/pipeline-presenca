import pandas as pd
import schema

class UnifiedPivotSheetGenerator:
    def __init__(self, report_kpi: pd.DataFrame, summary_data: dict):
        self.report_kpi = report_kpi
        self.summary_df = summary_data.get(schema.ABA_RESUMO_POR_ALUNO, pd.DataFrame())

    def generate(self) -> dict:
        if self.report_kpi.empty:
            return {
                'Painel_Coordenadores': pd.DataFrame()
            }

        df_gestao = self._generate_management_panel()

        return {
            'Painel_Coordenadores': df_gestao
        }

    def _generate_management_panel(self) -> pd.DataFrame:
        df_working = self.report_kpi.copy()
        
        if schema.COL_COORDINATOR in df_working.columns:
            df_working['coordinator_name'] = df_working[schema.COL_COORDINATOR].apply(
                lambda x: x.name if hasattr(x, 'name') else str(x)
            )
        else:
            return pd.DataFrame()

        if schema.OUT_COL_SITUACAO in df_working.columns:
            df_working['Atingiu_Bin'] = df_working[schema.OUT_COL_SITUACAO].apply(
                lambda x: 1 if x == schema.STATUS_ATINGIU else 0
            )
        else:
            return pd.DataFrame()

        painel = df_working.groupby(['coordinator_name', schema.COL_DATE]).agg(
            Total_Alunos=(schema.COL_NAME, 'count'),
            Alunos_Atingiram=('Atingiu_Bin', 'sum')
        ).reset_index()

        painel['Atingimento %'] = (painel['Alunos_Atingiram'] / painel['Total_Alunos']).fillna(0)
        painel['Atingimento %'] = (painel['Atingimento %'] * 100).round(1).astype(str) + '%'
        
        painel.rename(columns={'coordinator_name': 'Coordenador', schema.COL_DATE: 'Semana'}, inplace=True)
        painel.sort_values(by=['Coordenador', 'Semana'], inplace=True)

        return painel