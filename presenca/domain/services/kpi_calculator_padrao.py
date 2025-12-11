import pandas as pd
import numpy as np
import logging
from ...utils import schema
from .kpi_calculator_base import KpiCalculatorBase

log = logging.getLogger(__name__)

class KpiCalculatorPadrao(KpiCalculatorBase):
    
    def __init__(self, base_report: pd.DataFrame, processed_data: dict, config: dict):
        super().__init__(base_report, processed_data, config)
        self.base_report = base_report
        self.processed_data = processed_data
        self.config = config

    def calculate(self) -> pd.DataFrame:
        log.info("Calculadora KPI (Padrão): Inicializada.")
        
        if self.base_report.empty:
            log.warning("Calculadora KPI (Padrão): Relatório base está vazio, pulando cálculo.")
            return self.base_report

        report_kpi = self.base_report.copy()
        report_kpi[schema.COL_DATE] = pd.to_datetime(report_kpi[schema.COL_DATE], errors='coerce')
        
        report_kpi['year'] = report_kpi[schema.COL_DATE].dt.isocalendar().year
        report_kpi['week'] = report_kpi[schema.COL_DATE].dt.isocalendar().week

        report_kpi = self._apply_precise_frequency(report_kpi)
        
        freq_obs = report_kpi['observed_frequency']
        freq_esp = report_kpi['expected_frequency']
        faltas_just = report_kpi['justified_days']
        ferias = report_kpi.get('vacation_days', 0)
        
        deficit = freq_esp - freq_obs
        
        conditions = [
            (deficit <= 0),
            (deficit > 0) & (faltas_just >= deficit),
            (ferias >= 5)
        ]
        
        choices = [
            schema.STATUS_ATINGIU,
            schema.STATUS_JUSTIFICADO,
            schema.STATUS_JUSTIFICADO
        ]
        
        report_kpi[schema.OUT_COL_SITUACAO] = np.select(conditions, choices, default=schema.STATUS_NAO_ATINGIU)
        
        if 'year' in report_kpi.columns: report_kpi.drop(columns=['year'], inplace=True)
        if 'week' in report_kpi.columns: report_kpi.drop(columns=['week'], inplace=True)
        
        return report_kpi

    def _apply_precise_frequency(self, report_kpi: pd.DataFrame) -> pd.DataFrame:
        if 'observed_frequency' in report_kpi.columns:
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