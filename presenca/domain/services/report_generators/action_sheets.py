import pandas as pd
from datetime import date, timedelta, datetime
import logging
from typing import Dict, Set
import schema 

try:
    from ....utils.date_utils import get_workdays_for_week
except ImportError:
    def get_workdays_for_week(start_date: date, holidays: set) -> int:
        week_days = {start_date + timedelta(days=i) for i in range(5)}
        return 5 - len(week_days.intersection(holidays))

log = logging.getLogger(__name__)

class ActionSheetGenerator:

    def __init__(self, data_frames: dict, config: dict):
        self.data = data_frames
        self.config = config

    def generate(self) -> dict:
        tabs = {}
        start = pd.to_datetime(self.config.DATA_INICIO_GERAL).date()
        end = pd.to_datetime(self.config.DATA_FIM_GERAL).date()

        df_acoes = self._generate_action_sheet_filtered(start, end)
        tabs[schema.ABA_ACOES_CADASTRO] = df_acoes
        
        tabs.update(self._generate_raw_data_tabs(start, end))
        
        return tabs

    def _classify_status(self, nome_xml: str, 
                         set_cadastro_nome_entrada: Set[str], 
                         set_cadastro_nome_completo: Set[str], 
                         set_ignorar_nomes: Set[str], 
                         map_entrada_para_real: Dict[str, str]) -> str:
        
        if nome_xml in set_cadastro_nome_entrada:
            real_name = map_entrada_para_real.get(nome_xml)
            if real_name and real_name in set_ignorar_nomes:
                return schema.ACAO_OK_IGNORADO
            else:
                return schema.ACAO_OK_MATCH_CORRETO

        if nome_xml in set_ignorar_nomes:
            return schema.ACAO_OK_IGNORADO
            
        if nome_xml in set_cadastro_nome_completo:
            return schema.ACAO_CORRIGIR_NOME
            
        return schema.ACAO_NAO_CADASTRADO

    def _generate_action_sheet_filtered(self, start: date, end: date) -> pd.DataFrame:
        log.info("Gerando aba 'Acoes_de_Cadastro' (Apenas Problemas)...")
        
        df_registros = self.data.get('registros_brutos', pd.DataFrame())
        df_cadastro = self.data.get('cadastro', pd.DataFrame())
        df_ignorar = self.data.get('ignorar', pd.DataFrame())

        if df_registros.empty or schema.COL_XML_DATE not in df_registros.columns:
            log.warning("ActionSheet: 'registros_brutos' está vazio.")
            return pd.DataFrame(columns=[
                schema.OUT_COL_ACOES_NOME_XML, schema.OUT_COL_ACOES_SEMANA, 
                schema.OUT_COL_ACOES_FREQ_OBS, schema.OUT_COL_ACOES_SITUACAO
            ])

        set_ignorar_nomes = set(df_ignorar.iloc[:, 0].dropna().unique())
        set_cadastro_nome_entrada = set(df_cadastro[schema.COL_NOME_ENTRADA].dropna().unique())
        set_cadastro_nome_completo = set(df_cadastro[schema.COL_NAME].dropna().unique())
        
        map_entrada_para_real = {}
        if schema.COL_NOME_ENTRADA in df_cadastro.columns and schema.COL_NAME in df_cadastro.columns:
            map_entrada_para_real = df_cadastro.dropna(
                subset=[schema.COL_NOME_ENTRADA, schema.COL_NAME]
            ).set_index(schema.COL_NOME_ENTRADA)[schema.COL_NAME].to_dict()

        df_registros[schema.COL_XML_DATE] = pd.to_datetime(df_registros[schema.COL_XML_DATE]).dt.date
        df_xml_mes = df_registros[
            (df_registros[schema.COL_XML_DATE] >= start) & (df_registros[schema.COL_XML_DATE] <= end)
        ].copy()

        if df_xml_mes.empty:
            log.warning("ActionSheet: Nenhum registro XML no período.")
            return pd.DataFrame(columns=[
                schema.OUT_COL_ACOES_NOME_XML, schema.OUT_COL_ACOES_SEMANA, 
                schema.OUT_COL_ACOES_FREQ_OBS, schema.OUT_COL_ACOES_SITUACAO
            ])

        df_xml_mes[schema.OUT_COL_ACOES_SEMANA] = pd.to_datetime(
            df_xml_mes[schema.COL_XML_DATE]
        ).dt.to_period('W').apply(lambda r: r.start_time).dt.date
        
        report_base = df_xml_mes.groupby(
            [schema.COL_NOME_ENTRADA, schema.OUT_COL_ACOES_SEMANA]
        ).size().reset_index(name=schema.OUT_COL_ACOES_FREQ_OBS)

        report_base[schema.OUT_COL_ACOES_SITUACAO] = report_base[schema.COL_NOME_ENTRADA].apply(
            lambda nome: self._classify_status(
                nome,
                set_cadastro_nome_entrada,
                set_cadastro_nome_completo,
                set_ignorar_nomes,
                map_entrada_para_real
            )
        )
        
        situacoes_de_acao = [
            schema.ACAO_CORRIGIR_NOME,
            schema.ACAO_NAO_CADASTRADO
        ]
        report_final_acoes = report_base[
            report_base[schema.OUT_COL_ACOES_SITUACAO].isin(situacoes_de_acao)
        ].copy()
        
        if not report_final_acoes.empty:
            report_final_acoes.sort_values(
                by=[schema.OUT_COL_ACOES_SITUACAO, schema.COL_NOME_ENTRADA, schema.OUT_COL_ACOES_SEMANA], 
                inplace=True
            )
        
        return report_final_acoes

    def _generate_raw_data_tabs(self, start: date, end: date) -> dict:
        
        df_registros = self.data.get('registros_brutos', pd.DataFrame())
        renamed_xml = pd.DataFrame()
        
        if not df_registros.empty and schema.COL_XML_DATE in df_registros.columns:
            df_registros[schema.COL_XML_DATE] = pd.to_datetime(df_registros[schema.COL_XML_DATE]).dt.date
            
            xml_periodo = df_registros[
                (df_registros[schema.COL_XML_DATE] >= start) & (df_registros[schema.COL_XML_DATE] <= end)
            ].copy()
            
            if 'Datetime' in xml_periodo.columns:
                xml_periodo['Datetime'] = pd.to_datetime(
                    xml_periodo['Datetime'], errors='coerce'
                ).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            renamed_xml = xml_periodo.rename(columns={
                'nome_entrada': 'Nome (XML)', 'Datetime': 'Data e Hora (XML)',
                'Date': 'Data (XML)'
            })

        df_registros_final = self.data.get('registros_final', pd.DataFrame())
        raw_presence = pd.DataFrame()
        
        if not df_registros_final.empty and schema.COL_XML_DATE in df_registros_final.columns:
            df_registros_final[schema.COL_XML_DATE] = pd.to_datetime(df_registros_final[schema.COL_XML_DATE]).dt.date
            raw_presence = df_registros_final[
                (df_registros_final[schema.COL_XML_DATE] >= start) & (df_registros_final[schema.COL_XML_DATE] <= end)
            ].copy()
        
        return {
            schema.ABA_XML_EXPORT: renamed_xml,
            schema.ABA_RAW_PRESENCE: raw_presence,
            schema.ABA_IGNORAR: self.data.get('ignorar', pd.DataFrame())
        }