import pandas as pd
import numpy as np
import logging
import os
import unicodedata
from datetime import date
from ....utils import schema

log = logging.getLogger(__name__)

class BiometryCleanupSheetGenerator:
    
    def __init__(self, processed_data: dict, config: dict):
        self.registros_brutos = processed_data.get('registros_brutos', pd.DataFrame())
        self.cadastro = processed_data.get('cadastro', pd.DataFrame())
        self.ignorar = processed_data.get('ignorar', pd.DataFrame())
        self.config = config
        
        self.output_path = os.path.join("output", "output-dashboard")
        if hasattr(config, 'CAMINHOS') and 'local' in config.CAMINHOS:
             self.output_path = config.CAMINHOS['local'].get('output_dashboard', self.output_path)

    def generate(self) -> dict:
        log.info("Gerador Limpeza: Listando TODOS inativos (> 15 dias)...")

        ref_date = pd.Timestamp(self.config.DATA_FIM_GERAL)

        df_historico = self._load_history_names_and_dates()
        df_atual = self._get_current_names_and_dates()
        
        dfs_to_concat = []
        if not df_historico.empty: dfs_to_concat.append(df_historico)
        if not df_atual.empty: dfs_to_concat.append(df_atual)
        
        if not dfs_to_concat:
            return {schema.ABA_LIMPEZA_BIOMETRIA: pd.DataFrame()}
            
        df_full = pd.concat(dfs_to_concat, ignore_index=True)
        
        df_full['Data'] = pd.to_datetime(df_full['Data'], errors='coerce')
        df_full = df_full[df_full['Data'] <= ref_date].copy()

        if df_full.empty:
            return {schema.ABA_LIMPEZA_BIOMETRIA: pd.DataFrame()}

        df_full['nome_norm'] = self._normalize_name(df_full['Nome'])
        
        stats = df_full.groupby('nome_norm').agg({
            'Nome': 'first', 
            'Data': 'max'
        }).reset_index()

        stats['dias_off'] = (ref_date - stats['Data']).dt.days
        
        stats = stats[stats['dias_off'] > 15].copy()

        if stats.empty:
            log.info("Gerador Limpeza: Nenhum registro ausente há mais de 15 dias.")
            return {schema.ABA_LIMPEZA_BIOMETRIA: pd.DataFrame()}

        stats.rename(columns={
            'Nome': schema.OUT_COL_NOME_LIMPEZA,
            'Data': schema.OUT_COL_ULTIMA_PRESENCA_LIMPEZA,
            'dias_off': schema.OUT_COL_DIAS_INATIVO_LIMPEZA
        }, inplace=True)

        final_cols = [
            schema.OUT_COL_NOME_LIMPEZA, 
            schema.OUT_COL_ULTIMA_PRESENCA_LIMPEZA, 
            schema.OUT_COL_DIAS_INATIVO_LIMPEZA
        ]
        
        df_final = stats[final_cols].sort_values(by=schema.OUT_COL_DIAS_INATIVO_LIMPEZA, ascending=False)

        log.info(f"Gerador Limpeza: {len(df_final)} nomes listados (> 15 dias ausente).")
        
        return {schema.ABA_LIMPEZA_BIOMETRIA: df_final}

    def _load_history_names_and_dates(self) -> pd.DataFrame:
        try:
            filename = "STONE_LAB_DATABASE_HISTORICO.csv"
            path = os.path.join(self.output_path, filename)
            if not os.path.exists(path): return pd.DataFrame(columns=['Nome', 'Data'])
            
            df = pd.read_csv(path, usecols=[schema.DB_HIST_COL_NOME, schema.DB_HIST_COL_DATE])
            df.rename(columns={schema.DB_HIST_COL_NOME: 'Nome', schema.DB_HIST_COL_DATE: 'Data'}, inplace=True)
            df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
            return df.dropna()
        except Exception:
            return pd.DataFrame(columns=['Nome', 'Data'])

    def _get_current_names_and_dates(self) -> pd.DataFrame:
        if self.registros_brutos.empty or 'Name' not in self.registros_brutos.columns:
            return pd.DataFrame(columns=['Nome', 'Data'])
        
        df = self.registros_brutos[['Name', 'Datetime']].copy()
        df.rename(columns={'Name': 'Nome', 'Datetime': 'Data'}, inplace=True)
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        return df.dropna()

    def _normalize_name(self, series: pd.Series) -> pd.Series:
        s = series.astype(str).str.upper().str.strip().str.replace(r'\.0$', '', regex=True)
        return s.apply(lambda x: ''.join(c for c in unicodedata.normalize('NFD', x)
                                          if unicodedata.category(c) != 'Mn'))