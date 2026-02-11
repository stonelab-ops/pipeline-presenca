import pandas as pd
import schema

class DebtorsSheetGenerator:
    def __init__(self, summary_data: dict):
        self.summary_df = summary_data.get(schema.ABA_RESUMO_POR_ALUNO, pd.DataFrame())

    def generate(self) -> dict:
        if self.summary_df.empty:
            return {'Acao_Cobranca': pd.DataFrame()}

        if 'Situacao Geral no Mês' in self.summary_df.columns:
            mask_devedores = self.summary_df['Situacao Geral no Mês'] == schema.STATUS_NAO_ATINGIU
            df_devedores = self.summary_df[mask_devedores].copy()
        else:
            df_devedores = pd.DataFrame()

        if df_devedores.empty:
            return {'Acao_Cobranca': pd.DataFrame(columns=['Aluno', 'Atingimento %', 'Coordenador'])}

        col_map = {
            'Nome do Aluno': 'Aluno',
            'Atingimento %': 'Atingimento %',
            'Coordenador': 'Coordenador'
        }
        
        available_cols = [c for c in col_map.keys() if c in df_devedores.columns]
        df_final = df_devedores[available_cols].rename(columns=col_map)

        if 'Coordenador' in df_final.columns and 'Aluno' in df_final.columns:
            df_final.sort_values(by=['Coordenador', 'Aluno'], inplace=True)
            
        cols_order = ['Aluno', 'Atingimento %', 'Coordenador']
        cols_order = [c for c in cols_order if c in df_final.columns]
        df_final = df_final[cols_order]

        return {'Acao_Cobranca': df_final}