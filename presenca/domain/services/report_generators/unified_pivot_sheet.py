import pandas as pd
import schema

class UnifiedPivotSheetGenerator:
    def __init__(self, report_kpi: pd.DataFrame, summary_data: dict):
        self.report_kpi = report_kpi
        self.summary_df = summary_data.get(schema.ABA_RESUMO_POR_ALUNO, pd.DataFrame())

    def generate(self) -> dict:
        if self.report_kpi.empty:
            return {
                'Painel_Coordenadores': pd.DataFrame(),
                'Base_Analitica': pd.DataFrame()
            }

        df_analitico = self._generate_analytic_base()
        df_gestao = self._generate_management_panel(df_analitico)

        return {
            'Painel_Coordenadores': df_gestao,
            'Base_Analitica': df_analitico
        }

    def _generate_analytic_base(self) -> pd.DataFrame:
        df_week = self.report_kpi.copy()
        
        cols_week_target = [
            schema.COL_ID_STONELAB, schema.COL_NAME, schema.COL_COORDINATOR,
            schema.COL_DATE, 'observed_frequency', 'meta_dinamica', 
            schema.OUT_COL_SITUACAO, 'justified_days'
        ]
        cols_week = [c for c in cols_week_target if c in df_week.columns]
        df_week = df_week[cols_week]
        
        rename_map = {
            'observed_frequency': 'Presenca_Semana',
            'meta_dinamica': 'Meta_Semana',
            schema.OUT_COL_SITUACAO: 'Status_Semana',
            'justified_days': 'Dias_Justificados',
            schema.COL_COORDINATOR: 'Coordenador',
            schema.COL_DATE: 'Semana',
            schema.COL_NAME: 'Aluno'
        }
        df_week.rename(columns=rename_map, inplace=True)
        
        if 'Coordenador' in df_week.columns:
            df_week['Coordenador'] = df_week['Coordenador'].apply(
                lambda x: x.name if hasattr(x, 'name') else str(x)
            )

        if not self.summary_df.empty and 'Nome do Aluno' in self.summary_df.columns:
            df_month = self.summary_df[['Nome do Aluno', 'Situacao Geral no Mês', 'Atingimento %']].copy()
            
            df_final = pd.merge(
                df_week, df_month,
                left_on='Aluno', right_on='Nome do Aluno', how='left'
            )
            if 'Nome do Aluno' in df_final.columns:
                df_final.drop(columns=['Nome do Aluno'], inplace=True)
        else:
            df_final = df_week
            
        return df_final

    def _generate_management_panel(self, df_analitico: pd.DataFrame) -> pd.DataFrame:
        if df_analitico.empty:
            return pd.DataFrame()

        df_working = df_analitico.copy()
        df_working['Atingiu_Bin'] = df_working['Status_Semana'].apply(
            lambda x: 1 if x == schema.STATUS_ATINGIU else 0
        )

        painel = df_working.groupby(['Coordenador', 'Semana']).agg(
            Total_Alunos=('Aluno', 'count'),
            Alunos_Atingiram=('Atingiu_Bin', 'sum')
        ).reset_index()

        painel['Atingimento %'] = (painel['Alunos_Atingiram'] / painel['Total_Alunos']).fillna(0)
        
        painel['Atingimento %'] = (painel['Atingimento %'] * 100).round(1).astype(str) + '%'

        painel.sort_values(by=['Coordenador', 'Semana'], inplace=True)

        return painel