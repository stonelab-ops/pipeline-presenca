import pandas as pd
import numpy as np
import math
import logging
import schema
from datetime import timedelta
from .kpi_calculator_base import KpiCalculatorBase

log = logging.getLogger(__name__)

class KpiCalculatorPadrao(KpiCalculatorBase):
    
    def __init__(self, base_report: pd.DataFrame, processed_data: dict, config: dict):
        super().__init__(base_report, processed_data, config)
        self.base_report = base_report
        self.processed_data = processed_data
        self.config = config
        
        self.feriados_set = set()
        if 'feriados' in self.processed_data:
            df_fer = self.processed_data['feriados']
            if not df_fer.empty and 'Data' in df_fer.columns:
                self.feriados_set = set(pd.to_datetime(df_fer['Data'], errors='coerce').dt.date)

    def calculate(self) -> pd.DataFrame:
        log.info("Calculadora KPI (Padrão 60% Dinâmico): Inicializada.")
        
        if self.base_report.empty:
            log.warning("Calculadora KPI: Relatório base está vazio, pulando cálculo.")
            return self.base_report

        report_kpi = self.base_report.copy()
        
        report_kpi[schema.COL_DATE] = pd.to_datetime(report_kpi[schema.COL_DATE], errors='coerce')
        report_kpi['year'] = report_kpi[schema.COL_DATE].dt.isocalendar().year
        report_kpi['week'] = report_kpi[schema.COL_DATE].dt.isocalendar().week
        report_kpi = self._apply_precise_frequency(report_kpi)
        report_kpi['meta_dinamica'] = report_kpi.apply(self._calculate_weekly_target, axis=1)

        freq_obs = report_kpi['observed_frequency']
        meta = report_kpi['meta_dinamica']
        faltas_just = report_kpi.get('justified_days', 0).fillna(0)
        
        conditions = [
            (freq_obs >= meta),                      
            (freq_obs < meta) & (faltas_just > 0)    
        ]
        
        choices = [
            schema.STATUS_ATINGIU,
            schema.STATUS_JUSTIFICADO
        ]
        
        report_kpi[schema.OUT_COL_SITUACAO] = np.select(conditions, choices, default=schema.STATUS_NAO_ATINGIU)
        
        if 'year' in report_kpi.columns: report_kpi.drop(columns=['year'], inplace=True)
        if 'week' in report_kpi.columns: report_kpi.drop(columns=['week'], inplace=True)
        
        return report_kpi

    def _calculate_weekly_target(self, row) -> int:
        """
        Calcula a meta de presença para uma semana específica, considerando:
        1. Frequência Esperada do Aluno (Ratio)
        2. Dias Úteis Reais (descontando Feriados da lista)
        3. Arredondamento para Cima (Teto)
        """
        try:
            year = int(row['year'])
            week = int(row['week'])
            freq_esp = float(row.get('expected_frequency', 3))
            ratio = freq_esp / 5.0
            monday = pd.Timestamp.fromisocalendar(year, week, 1).date()
            dias_uteis_count = 0
            
            for i in range(5): 
                current_date = monday + timedelta(days=i)
                
                if current_date not in self.feriados_set:
                    dias_uteis_count += 1
            
            if dias_uteis_count == 0:
                return 0
            
            meta = math.ceil(dias_uteis_count * ratio)
            
            return int(meta)
            
        except Exception as e:
            log.error(f"Erro ao calcular meta dinâmica para {row.get(schema.COL_NAME, 'Unknown')}: {e}")
            return 3 

    def _apply_precise_frequency(self, report_kpi: pd.DataFrame) -> pd.DataFrame:
        if 'observed_frequency' in report_kpi.columns:
            report_kpi['observed_frequency'] = report_kpi['observed_frequency'].fillna(0).astype(int)
            return report_kpi
            
        log.info("Calculadora KPI: Recalculando frequência observada (Fallback)...")
        
        registros = self.processed_data.get('registros_final', pd.DataFrame())
        if registros.empty:
            report_kpi['observed_frequency'] = 0
            return report_kpi
            
        registros['Date'] = pd.to_datetime(registros[schema.COL_XML_DATE], errors='coerce')
        registros['year'] = registros['Date'].dt.isocalendar().year
        registros['week'] = registros['Date'].dt.isocalendar().week
        
        freq_obs_precisa = registros.groupby(
            [schema.COL_ID_STONELAB, 'year', 'week']
        ).size().reset_index(name='observed_frequency')
        
        report_kpi = pd.merge(
            report_kpi, 
            freq_obs_precisa,
            on=[schema.COL_ID_STONELAB, 'year', 'week'], 
            how='left'
        )
        
        report_kpi['observed_frequency'] = report_kpi['observed_frequency'].fillna(0).astype(int)
        return report_kpi