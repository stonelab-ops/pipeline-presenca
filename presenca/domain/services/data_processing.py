import pandas as pd
from typing import Dict
from ..factory import TenureFactory
from ..models.tenure import Tenure
import logging

log = logging.getLogger(__name__)

class DataProcessingService:
    def __init__(self, data_frames: dict, config: dict):
        self.data = data_frames
        self.config = config
        self.tenure_factory = TenureFactory()
        log.info("Serviço de Processamento de Dados inicializado.")

    def run(self) -> dict:
        """Orquestra o fluxo de processamento e retorna os dados limpos."""
        self._clean_and_rename_base_dfs()
        self._apply_ignore_list()
        self._filter_by_tenure()        
        return self.data

    def _clean_and_rename_base_dfs(self):
        """
        Limpa e renomeia os DataFrames base.
        Esta função agora processa 'cadastro' mesmo se 'registros_brutos' estiver vazio.
        """
        
        # --- Bloco 1: Processar Registros Brutos (XMLs) ---
        df_registros = self.data['registros_brutos']
        if not df_registros.empty:
            df_registros.dropna(subset=['Datetime', 'Name'], inplace=True)
            df_registros['Date'] = pd.to_datetime(
                df_registros['Datetime'].str.split(' ').str[0],
                errors='coerce'
            ).dt.date
            df_registros.dropna(subset=['Date'], inplace=True)
            df_registros.drop_duplicates(subset=['Name', 'Date'], inplace=True)
            df_registros.rename(columns={'Name': 'nome_entrada'}, inplace=True)
            self.data['registros_brutos'] = df_registros
        
        # --- Bloco 2: Processar Cadastro (CSV) ---
        df_depara = self.data['cadastro']
        df_depara.columns = df_depara.columns.str.strip()
        df_depara.rename(columns={
            'Qual o seu nome completo?': 'name',
            'Qual a sua função no projeto?': 'function',
            'Quem é o professor responsável pelo seu projeto?': 'coordinator',
            'id_stonelab': 'id_stonelab'
        }, inplace=True)
        self.data['cadastro'] = df_depara

    def _apply_ignore_list(self):
        df_ignorar = self.data['ignorar']
        df_depara = self.data['cadastro']
        
        set_ignorar_raw = set(df_ignorar.iloc[:, 0].dropna().unique())
        
        set_cadastro_nomes = set(df_depara['name'].dropna().unique())
        if 'nome_entrada' in df_depara.columns:
             set_cadastro_nomes.union(
                set(df_depara['nome_entrada'].dropna().unique())
             )
             
        self.data['ignorar_final'] = set_ignorar_raw - set_cadastro_nomes
        
        if not self.data['registros_brutos'].empty:
            self.data['registros_brutos'] = self.data['registros_brutos'][
                ~self.data['registros_brutos']['nome_entrada'].isin(
                    self.data['ignorar_final']
                )
            ].copy()

    def _filter_by_tenure(self):
        tenures = self.tenure_factory.create_tenures_from_df(
            self.data['io_alunos']
        )
        self.data['tenures'] = tenures
        
        if self.data['registros_brutos'].empty:
            self.data['registros_final'] = pd.DataFrame()
            return

        df_registros_alunos = pd.merge(
            self.data['registros_brutos'], self.data['cadastro'],
            on='nome_entrada', how='inner'
        )
        ids_com_jornada = list(tenures.keys())
        df_com_jornada = df_registros_alunos[
            df_registros_alunos['id_stonelab'].astype(str).isin(ids_com_jornada)
        ].copy()

        def is_active(row): return self._is_active_at_date(row, tenures)
        
        if not df_com_jornada.empty:
            df_com_jornada['active'] = df_com_jornada.apply(is_active, axis=1)
            self.data['registros_final'] = df_com_jornada[
                df_com_jornada['active']
            ].copy()
        else:
             self.data['registros_final'] = pd.DataFrame()

    def _is_active_at_date(self, row, tenures_dict):
        sid = str(row['id_stonelab'])
        pdate = pd.to_datetime(row['Date']).date()
        if sid in tenures_dict:
            for tenure in tenures_dict[sid]:
                if tenure.active_at_date(pdate):
                    return True
        return False