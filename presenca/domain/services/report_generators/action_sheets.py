import pandas as pd
from datetime import date, timedelta, datetime
import logging
from typing import Dict, Set

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
        tabs['Acoes_de_Cadastro'] = df_acoes
        
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
                return "OK (Ignorado)"
            else:
                return "OK (Match Correto)"

        if nome_xml in set_ignorar_nomes:
            return "OK (Ignorado)"
            
        if nome_xml in set_cadastro_nome_completo:
            return "(1) Corrigir 'nome_entrada' no Cadastro"
            
        return "(2) Pessoa Não Cadastrada (Verificar)"

    def _generate_action_sheet_filtered(self, start: date, end: date) -> pd.DataFrame:
        log.info("Gerando aba 'Acoes_de_Cadastro' (Apenas Problemas)...")
        
        df_registros = self.data.get('registros_brutos', pd.DataFrame())
        df_cadastro = self.data.get('cadastro', pd.DataFrame())
        df_ignorar = self.data.get('ignorar', pd.DataFrame())

        if df_registros.empty or 'Date' not in df_registros.columns:
            log.warning("ActionSheet: 'registros_brutos' está vazio.")
            return pd.DataFrame(columns=['nome_entrada', 'Semana', 'Freq Obs', 'Situacao'])

        set_ignorar_nomes = set(df_ignorar.iloc[:, 0].dropna().unique())
        set_cadastro_nome_entrada = set(df_cadastro['nome_entrada'].dropna().unique())
        set_cadastro_nome_completo = set(df_cadastro['name'].dropna().unique())
        
        map_entrada_para_real = {}
        if 'nome_entrada' in df_cadastro.columns and 'name' in df_cadastro.columns:
            map_entrada_para_real = df_cadastro.dropna(subset=['nome_entrada', 'name']).set_index('nome_entrada')['name'].to_dict()

        df_registros['Date'] = pd.to_datetime(df_registros['Date']).dt.date
        df_xml_mes = df_registros[
            (df_registros['Date'] >= start) & (df_registros['Date'] <= end)
        ].copy()

        if df_xml_mes.empty:
            log.warning("ActionSheet: Nenhum registro XML no período.")
            return pd.DataFrame(columns=['nome_entrada', 'Semana', 'Freq Obs', 'Situacao'])

        df_xml_mes['Semana'] = pd.to_datetime(
            df_xml_mes['Date']
        ).dt.to_period('W').apply(lambda r: r.start_time).dt.date
        
        report_base = df_xml_mes.groupby(
            ['nome_entrada', 'Semana']
        ).size().reset_index(name='Freq Obs')

        report_base['Situacao'] = report_base['nome_entrada'].apply(
            lambda nome: self._classify_status(
                nome,
                set_cadastro_nome_entrada,
                set_cadastro_nome_completo,
                set_ignorar_nomes,
                map_entrada_para_real
            )
        )
        
        report_final_acoes = report_base[
            report_base['Situacao'].str.startswith('(')
        ].copy()
        
        if not report_final_acoes.empty:
            report_final_acoes.sort_values(by=['Situacao', 'nome_entrada', 'Semana'], inplace=True)
        
        return report_final_acoes

    def _generate_raw_data_tabs(self, start: date, end: date) -> dict:
        
        df_registros = self.data.get('registros_brutos', pd.DataFrame())
        renamed_xml = pd.DataFrame()
        
        if not df_registros.empty and 'Date' in df_registros.columns:
            df_registros['Date'] = pd.to_datetime(df_registros['Date']).dt.date
            
            xml_periodo = df_registros[
                (df_registros['Date'] >= start) & (df_registros['Date'] <= end)
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
        
        if not df_registros_final.empty and 'Date' in df_registros_final.columns:
            df_registros_final['Date'] = pd.to_datetime(df_registros_final['Date']).dt.date
            raw_presence = df_registros_final[
                (df_registros_final['Date'] >= start) & (df_registros_final['Date'] <= end)
            ].copy()
        
        return {
            'export_XMLs': renamed_xml,
            'raw_presence_data': raw_presence,
            'Pessoas para nao monitorar': self.data.get('ignorar', pd.DataFrame())
        }